// =-=-=-=-=-=-=-
// local includes
#include "s3_archive_operations.hpp"
#include "libirods_s3.hpp"
#include "s3fs/curl.h"
#include "s3fs/cache.h"
#include "s3fs/fdcache.h"
#include "s3fs/s3fs.h"
#include "s3fs/s3fs_util.h"
#include "s3fs/s3fs_auth.h"
#include "s3fs/common.h"
#include "s3_transport.hpp"


// =-=-=-=-=-=-=-
// irods includes
#include <msParam.h>
#include <rcConnect.h>
#include <rodsLog.h>
#include <rodsErrorTable.h>
#include <objInfo.h>
#include <rsRegReplica.hpp>
#include <dataObjOpr.hpp>
#include <irods_string_tokenize.hpp>
#include <irods_resource_plugin.hpp>
#include <irods_resource_redirect.hpp>
#include <irods_collection_object.hpp>
#include <irods_stacktrace.hpp>
#include <irods_random.hpp>
#include <irods/irods_resource_backport.hpp>
#include <dstream.hpp>

// =-=-=-=-=-=-=-
// boost includes
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/filesystem/path.hpp>

// =-=-=-=-=-=-=-
// other includes
#include <string>
#include <iomanip>
#include <fcntl.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/tree.h>
#include <cstdlib>

#include <curl/curl.h>


extern size_t g_retry_count;
extern size_t g_retry_wait;

extern S3ResponseProperties savedProperties;

using odstream            = irods::experimental::io::odstream;
using idstream            = irods::experimental::io::idstream;
using dstream             = irods::experimental::io::dstream;
using s3_transport        = irods::experimental::io::s3_transport::s3_transport<char>;
using s3_transport_config = irods::experimental::io::s3_transport::config;

namespace irods_s3_cacheless {

    std::mutex s3_cacheless_plugin_mutex;     // to protect the following shared variables
    int fd_counter = 3;
    int overwrite_open_mode = 0; // used to overwrite open mode if necessary
    std::map<int, int> fd_to_open_mode_map;

    // need map because the close can occur in a different thread than the
    // read/write/seek operations
    std::map<int, std::shared_ptr<dstream>> file_descriptor_to_dstream_map;
    std::map<int, std::shared_ptr<s3_transport>> file_descriptor_to_transport_map;


    std::ios_base::openmode translate_open_mode_posix_to_stream(int oflag, const std::string& call_from) noexcept
    {
        using std::ios_base;

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

        printf("%s:%d (%s) [[%lu]] call_from=%s oflag=%d oflag&O_TRUNC=%d oflag&O_CREAT=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, call_from.c_str(), oflag, oflag & O_TRUNC, oflag & O_CREAT);
        printf("%s:%d (%s) O_WRONLY=%d, O_RDWR=%d, O_RDONLY=%d, O_TRUNC=%d, O_CREAT=%d, O_APPEND=%d\n", __FILE__, __LINE__, __FUNCTION__,
               O_WRONLY, O_RDWR, O_RDONLY, O_TRUNC, O_CREAT, O_APPEND);

        ios_base::openmode mode = 0;

        if ((oflag & O_ACCMODE) == O_WRONLY || (oflag & O_ACCMODE) == O_RDWR) {
            mode |= ios_base::out;
        }

        if ((oflag & O_ACCMODE) == O_RDONLY || (oflag & O_ACCMODE) == O_RDWR) {
            mode |= ios_base::in;
        }

        if ((oflag & O_TRUNC) || (oflag & O_CREAT)) {
            mode |= ios_base::trunc;
        }

        if (oflag & O_APPEND) {
            mode |= ios_base::app;
        }

        return mode;

    }

    irods::error make_dstream(irods::plugin_context& _ctx, const std::string& call_from)
    {

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        printf("%s:%d (%s) [[%lu]] call_from=%s\n", __FILE__, __LINE__, __FUNCTION__, thread_id, call_from.c_str());
        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // get the file descriptor
        int fd = file_obj->file_descriptor();

        // if already done just return
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            if (file_descriptor_to_dstream_map.find(fd) != file_descriptor_to_dstream_map.end()) {
                return SUCCESS();
            }
        }

        std::string bucket_name;
        std::string object_key;
        std::string access_key;
        std::string secret_access_key;

        irods::error ret = parseS3Path(file_obj->physical_path(), bucket_name, object_key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        printf("%s:%d (%s) [physical_path=%s][bucket_name=%s]\n", __FILE__, __LINE__, __FUNCTION__, file_obj->physical_path().c_str(), bucket_name.c_str());

        ret = s3GetAuthCredentials(_ctx.prop_map(), access_key, secret_access_key);
        if(!ret.ok()) {
            return PASS(ret);
        }
        printf("%s:%d (%s) [access_key=%s][secret_access_key=%s]\n", __FILE__, __LINE__, __FUNCTION__, access_key.c_str(), secret_access_key.c_str());

        // get open mode
        std::ios_base::openmode open_mode;
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            open_mode = fd_to_open_mode_map[fd];
        }

        // get the file size
        uint64_t data_size = 0;
        ret = _ctx.prop_map().get< uint64_t >(DATA_SIZE_KW, data_size);
        if (!ret.ok()) {
            data_size = 0;
        }

        // get number of threads
        for (int i = 0; i < NUM_L1_DESC; ++i) {
           if (L1desc[i].inuseFlag) {
               printf("%s:%d (%s) [[%lu]] l1descInx=%d, numThreads=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, i, L1desc[i].dataObjInp->numThreads);
           }
        }

        int requested_number_of_threads = 0;
        int l1descInx = file_obj->l1_desc_idx();
        printf("%s:%d (%s) [[%lu]] l1descInx=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, l1descInx);
        if (l1descInx >= 3) {
            requested_number_of_threads = L1desc[l1descInx].dataObjInp->numThreads;
            printf("%s:%d (%s) [[%lu]] requested_number_of_threads=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, requested_number_of_threads);
        }

        int number_of_threads = getNumThreads( _ctx.comm(),
                data_size,
                requested_number_of_threads,
                const_cast<KeyValPair*>(&file_obj->cond_input()),
                nullptr,                     // destination resc hier
                nullptr,                     // source resc hier
                0 );                         // opr type - not used


        s3_transport_config s3_config;
        s3_config.object_size = data_size;
        s3_config.number_of_transfer_threads = 20;
        s3_config.part_size = data_size / number_of_threads;
        s3_config.bucket_name = bucket_name;
        s3_config.access_key = access_key;
        s3_config.secret_access_key = secret_access_key;
        s3_config.debug_flag = true;
        s3_config.multipart_upload_flag = true;
        s3_config.shared_memory_timeout_in_seconds = 60;

        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            file_descriptor_to_transport_map[fd] = std::make_shared<s3_transport>(s3_config);
            file_descriptor_to_dstream_map[fd] = std::make_shared<dstream>(*(file_descriptor_to_transport_map[fd]), object_key, open_mode);
        }

        /*if (!dstream_ptr->is_open()) {
            return ERROR(S3_FILE_OPEN_ERR, "Failed to create dstream.");
        }*/

        return SUCCESS();
    }

    irods::error set_s3_configuration_from_context(irods::plugin_property_map& _prop_map) {

        static bool already_destroyed = false;

        // this is taken from s3fs.cpp - main() with adjustments

        irods::error ret = s3Init( _prop_map );
        if (!ret.ok()) {
            return PASS(ret);
        }

        // get keys
        std::string key_id, access_key;
        ret = _prop_map.get< std::string >(s3_key_id, key_id);
        if (!ret.ok()) {
            if (!already_destroyed) {
                already_destroyed = true;
                S3fsCurl::DestroyS3fsCurl();
                s3fs_destroy_global_ssl();
            }
            std::string error_str =  boost::str(boost::format("[resource_name=%s] failed to read S3_ACCESS_KEY_ID.")
                        % get_resource_name(_prop_map).c_str());
            rodsLog(LOG_ERROR, error_str.c_str());
            return ERROR(S3_INIT_ERROR, error_str.c_str());
        }

        ret = _prop_map.get< std::string >(s3_access_key, access_key);
        if (!ret.ok()) {
            if (!already_destroyed) {
                already_destroyed = true;
                S3fsCurl::DestroyS3fsCurl();
                s3fs_destroy_global_ssl();
            }
            std::string error_str =  boost::str(boost::format("[resource_name=%s] failed to read S3_SECRET_ACCESS_KEY.")
                        % get_resource_name(_prop_map).c_str());
            rodsLog(LOG_ERROR, error_str.c_str());
            return ERROR(S3_INIT_ERROR, error_str.c_str());
        }

        // save keys
        if(!S3fsCurl::SetAccessKey(key_id.c_str(), access_key.c_str())){
            if (!already_destroyed) {
                already_destroyed = true;
                S3fsCurl::DestroyS3fsCurl();
                s3fs_destroy_global_ssl();
            }

            std::string error_str =  boost::str(boost::format("[resource_name=%s] failed to set internal data for access key/secret key.")
                        % get_resource_name(_prop_map).c_str());
            rodsLog(LOG_ERROR, error_str.c_str());
            return ERROR(S3_INIT_ERROR, error_str.c_str());
        }
        S3fsCurl::InitUserAgent();

        ret = _prop_map.get< std::string >(s3_proto, s3_protocol_str);
        if (!ret.ok()) {
            if (!already_destroyed) {
                already_destroyed = true;
                S3fsCurl::DestroyS3fsCurl();
                s3fs_destroy_global_ssl();
            }

            std::string error_str =  boost::str(boost::format("[resource_name=%s] S3_PROTO is not defined for resource.")
                        % get_resource_name(_prop_map).c_str());
            rodsLog(LOG_ERROR, error_str.c_str());
            return ERROR(S3_INIT_ERROR, error_str.c_str());
        }

        // if cachedir is defined, use that else use /tmp/<resc_name>
        std::string s3_cache_dir_str;
        ret = _prop_map.get< std::string >(s3_cache_dir, s3_cache_dir_str);
        if (!ret.ok()) {
            const auto& shared_memory_name_salt = irods::get_server_property<const std::string>(irods::CFG_RE_CACHE_SALT_KW);
            std::string resc_name  = "";
            ret = _prop_map.get< std::string >( irods::RESOURCE_NAME, resc_name);
            s3_cache_dir_str = "/tmp/" + resc_name + shared_memory_name_salt;
            _prop_map.set< std::string >(s3_cache_dir, s3_cache_dir_str);
        }
        FdManager::SetCacheDir(s3_cache_dir_str);

        if (boost::iequals(s3_protocol_str, "https")) {
            s3_protocol_str = "https";
        } else if (boost::iequals(s3_protocol_str, "http")) {
            s3_protocol_str = "http";
        } else {
            s3_protocol_str = "";
        }

        S3SignatureVersion signature_version = s3GetSignatureVersion(_prop_map);

        if (signature_version == S3SignatureV4) {
            S3fsCurl::SetSignatureV4(true);
        } else {
            S3fsCurl::SetSignatureV4(false);
        }

        nomultipart = !s3GetEnableMultiPartUpload(_prop_map);

        // set multipart size
        //    Note:  SetMultipartSize takes value in MB so need to convert back from bytes to MB.
        S3fsCurl::SetMultipartSize(s3GetMPUChunksize(_prop_map) / (1024ULL * 1024ULL));

        // set number of simultaneous threads
        S3fsCurl::SetMaxParallelCount(s3GetMPUThreads(_prop_map));

        // set the MD5 flag
        S3fsCurl::SetContentMd5(s3GetEnableMD5(_prop_map));

        //service_path = "";
        strncpy(host, s3GetHostname(_prop_map).c_str(), MAX_NAME_LEN-1);

        std::string endpoint_str;
        _prop_map.get< std::string >(s3_region_name, endpoint_str); // if this fails use default
        strncpy(endpoint, endpoint_str.c_str(), MAX_NAME_LEN-1);

        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for file registration
    irods::error s3RegisteredPlugin( irods::plugin_context& _ctx) {
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for file unregistration
    irods::error s3UnregisteredPlugin( irods::plugin_context& _ctx) {
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for file modification
    irods::error s3ModifiedPlugin( irods::plugin_context& _ctx) {
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX create

    irods::error s3FileCreatePlugin( irods::plugin_context& _ctx) {

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        std::ios_base::openmode open_mode = translate_open_mode_posix_to_stream(O_CREAT | O_WRONLY, __FUNCTION__);

        printf("%s:%d (%s) [[%lu]] open_mode=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, file_obj->flags());
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            int fd = fd_counter++;
            file_obj->file_descriptor(fd);
            fd_to_open_mode_map[fd] = open_mode;
        }

        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    irods::error s3FileOpenPlugin( irods::plugin_context& _ctx) {

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

        printf("%s:%d (%s) [[%lu]] open_mode=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, file_obj->flags());

        std::ios_base::openmode open_mode;
        if (0 != overwrite_open_mode) {
            open_mode = translate_open_mode_posix_to_stream(overwrite_open_mode, __FUNCTION__);
        } else {
            open_mode = translate_open_mode_posix_to_stream(file_obj->flags(), __FUNCTION__);
        }

        // save fd and open mode
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            int fd = fd_counter++;
            file_obj->file_descriptor(fd);
            fd_to_open_mode_map[fd] = open_mode;
        }

        return SUCCESS();

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    irods::error s3FileReadPlugin( irods::plugin_context& _ctx,
                                   void*               _buf,
                                   int                 _len ) {

        irods::error result = SUCCESS();

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        result = make_dstream(_ctx, __FUNCTION__);

        if (!result.ok()) {
            return PASS(result);
        }

        int fd = file_obj->file_descriptor();
        std::shared_ptr<dstream> dstream_ptr;
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            dstream_ptr = file_descriptor_to_dstream_map[fd];
        }

        if (!dstream_ptr || !dstream_ptr->is_open()) {
            return ERROR(S3_FILE_OPEN_ERR, "No valid dstream found.");
        }

        off_t offset = dstream_ptr->tellg();
        printf("%s:%d (%s) [[%lu]] offset=%ld, len=%d\n", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()), offset, _len);

        dstream_ptr->read(static_cast<char*>(_buf), _len);

        result.code(_len);
        return result;

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    irods::error s3FileWritePlugin( irods::plugin_context& _ctx,
                                    void*               _buf,
                                    int                 _len ) {

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        printf("%s:%d (%s) [[%lu]]\n", __FILE__, __LINE__, __FUNCTION__, thread_id);

        irods::error result = SUCCESS();

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
        int fd = file_obj->file_descriptor();

        // make and read dstream_ptr
        result = make_dstream(_ctx, __FUNCTION__);

        if (!result.ok()) {
            return PASS(result);
        }

        std::shared_ptr<dstream> dstream_ptr;
        std::shared_ptr<s3_transport> s3_transport_ptr;
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            dstream_ptr = file_descriptor_to_dstream_map[fd];
            s3_transport_ptr = file_descriptor_to_transport_map[fd];
        }

        if (!dstream_ptr || !dstream_ptr->is_open()) {
            return ERROR(S3_FILE_OPEN_ERR, "No valid dstream found.");
        }

        std::stringstream msg;

        if (!s3_transport_ptr) {
            msg << "No valid dstream found for fd " << fd;
            return ERROR(S3_FILE_OPEN_ERR, msg.str());
        }

        off_t offset = dstream_ptr->tellg();
        printf("%s:%d (%s) [[%lu]] offset=%ld, len=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, offset, _len);

        uint64_t data_size = 0;
        irods::error ret = _ctx.prop_map().get< uint64_t >(DATA_SIZE_KW, data_size);
        if (!ret.ok()) {
            data_size = 0;
        }

        int requested_number_of_threads = 0;
        int l1descInx = file_obj->l1_desc_idx();
        printf("%s:%d (%s) [[%lu]] l1descInx=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, l1descInx);
        if (l1descInx >= 3) {
            requested_number_of_threads = L1desc[l1descInx].dataObjInp->numThreads;
            printf("%s:%d (%s) [[%lu]] requested_number_of_threads=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, requested_number_of_threads);
        }

        int number_of_threads = getNumThreads( _ctx.comm(),
                data_size,
                requested_number_of_threads,
                const_cast<KeyValPair*>(&file_obj->cond_input()),
                nullptr,                     // destination resc hier
                nullptr,                     // source resc hier
                0 );                         // opr type - not used

        // determine the part size based on the offset
        uint64_t part_size = data_size / number_of_threads;
        if (static_cast<uint64_t>(offset) >= part_size * (number_of_threads-1)) {
            part_size += data_size % number_of_threads;
        }
        printf("%s:%d (%s) [[%lu]] offset=%ld,part_size=%ld\n", __FILE__, __LINE__, __FUNCTION__, thread_id, offset, part_size);


        s3_transport_ptr->set_part_size(part_size);

        dstream_ptr->write(static_cast<char*>(_buf), _len);
        offset = dstream_ptr->tellg();
        printf("%s:%d (%s) [[%lu]] after write offset=%ld\n", __FILE__, __LINE__, __FUNCTION__, thread_id, offset);

        result.code(_len);
        return result;

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error s3FileClosePlugin(  irods::plugin_context& _ctx ) {

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        printf("%s:%d (%s) [[%lu]]\n", __FILE__, __LINE__, __FUNCTION__, thread_id);

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        int fd = file_obj->file_descriptor();

        if (fd == 0) {
            return SUCCESS();
        }

        std::shared_ptr<dstream> dstream_ptr;
        std::shared_ptr<s3_transport> s3_transport_ptr;

        // if dstream doesn't exist just return
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            fd_to_open_mode_map.erase(fd);
            if (file_descriptor_to_dstream_map.end() == file_descriptor_to_dstream_map.find(fd)) {
                return SUCCESS();
            }

            dstream_ptr = file_descriptor_to_dstream_map[fd];
        }
        //printf("%s:%d (%s) [[%lu]] remove dstream_ptr fd=%d dstream_ptr=%p\n", __FILE__, __LINE__, __FUNCTION__, thread_id, fd, dstream_ptr.get());

        if (dstream_ptr && dstream_ptr->is_open()) {
            dstream_ptr->close();
        }

        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            //file_descriptor_to_dstream_map.erase(fd);
            //file_descriptor_to_transport_map.erase(fd);
        }

        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    irods::error s3FileUnlinkPlugin(
        irods::plugin_context& _ctx) {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        int result;

        S3fsCurl s3fscurl;
        result = s3fscurl.DeleteRequest(key.c_str());
        FdManager::DeleteCacheFile(key.c_str());
        StatCache::getStatCacheData()->DelStat(key.c_str());
        S3FS_MALLOCTRIM(0);

        if (result < 0) {
          return ERROR(S3_FILE_UNLINK_ERR, boost::str(boost::format("[resource_name=%s] Could not unlink file %s")
                      % get_resource_name(_ctx.prop_map()).c_str() % key.c_str()));
        }
        return SUCCESS();

    } // s3FileUnlinkPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Stat
    irods::error s3FileStatPlugin(
        irods::plugin_context& _ctx,
        struct stat* _statbuf )
    {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }
        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        int returnVal;
        returnVal = get_object_attribute(key.c_str(), _statbuf);
        if (0 != returnVal) {
            return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Failed to perform a stat of %s")
                        % get_resource_name(_ctx.prop_map()).c_str() % key.c_str()));
        }

        // If has already opened fd, the st_size should be instead.
        // (See: Issue 241)
        if(_statbuf){
          FdEntity*   ent;

          if(NULL != (ent = FdManager::get()->ExistOpen(key.c_str()))){
            struct stat tmpstbuf;
            if(ent->GetStats(tmpstbuf)){
              _statbuf->st_size = tmpstbuf.st_size;
            }
          }
          _statbuf->st_blksize = 4096;
          _statbuf->st_blocks  = get_blocks(_statbuf->st_size);
          S3FS_PRN_DBG("[path=%s] uid=%u, gid=%u, mode=%04o", key.c_str(), (unsigned int)(_statbuf->st_uid), (unsigned int)(_statbuf->st_gid), _statbuf->st_mode);
        }
        S3FS_MALLOCTRIM(0);

        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Fstat
    irods::error s3FileFstatPlugin(  irods::plugin_context& _ctx,
                                     struct stat*           _statbuf ) {
        return SUCCESS();

    } // s3FileFstatPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    irods::error s3FileLseekPlugin(  irods::plugin_context& _ctx,
                                     long long              _offset,
                                     int                    _whence ) {

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        printf("%s:%d (%s) [[%lu]]\n", __FILE__, __LINE__, __FUNCTION__, thread_id);

        irods::error result = SUCCESS();

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // read fd
        int fd = file_obj->file_descriptor();

        result = make_dstream(_ctx, __FUNCTION__);

        if (!result.ok()) {
            return PASS(result);
        }

        std::shared_ptr<dstream> dstream_ptr;
        std::shared_ptr<s3_transport> s3_transport_ptr;
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            dstream_ptr = file_descriptor_to_dstream_map[fd];
            s3_transport_ptr = file_descriptor_to_transport_map[fd];
        }

        if (!dstream_ptr || !dstream_ptr->is_open()) {
            return ERROR(S3_FILE_OPEN_ERR, "No valid dstream found.");
        }

        std::stringstream msg;

        if (!s3_transport_ptr) {
            msg << "No valid transport found for fd " << fd;
            return ERROR(S3_FILE_OPEN_ERR, msg.str());
        }

        printf("%s:%d (%s) [[%lu]] offset=%lld\n", __FILE__, __LINE__, __FUNCTION__, thread_id, _offset);

        std::ios_base::seekdir seek_directive =
            _whence == SEEK_SET ? std::ios_base::beg : (
                    _whence == SEEK_END ? std::ios_base::end : std::ios_base::cur);

        dstream_ptr->seekg(_offset, seek_directive);

        int pos = dstream_ptr->tellg();
        result.code(pos);

        printf("%s:%d (%s) [[%lu] tellg=%d\n", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()), pos);

        return result;

    } // s3FileLseekPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3FileMkdirPlugin(  irods::plugin_context& _ctx ) {
        return SUCCESS();

    } // s3FileMkdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3FileRmdirPlugin(  irods::plugin_context& _ctx ) {
        return SUCCESS();
    } // s3FileRmdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    irods::error s3FileOpendirPlugin( irods::plugin_context& _ctx ) {
        return SUCCESS();
    } // s3FileOpendirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    irods::error s3FileClosedirPlugin( irods::plugin_context& _ctx) {
        return SUCCESS();
    } // s3FileClosedirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error s3FileReaddirPlugin( irods::plugin_context& _ctx,
                                      struct rodsDirent**     _dirent_ptr ) {


        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if (!ret.ok()) {
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::collection_object_ptr fco = boost::dynamic_pointer_cast< irods::collection_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        ret = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        key = "/" + key;

        S3ObjList head;
        int result;

        S3FS_PRN_INFO("[path=%s]", path.c_str());

        if (!(DirectoryListStreamManager::get()->key_exists(key))) {

            // Do not have entries cached.  Go ahead and retrieve them

            // get a list of all the objects
            if ((result = list_bucket(key.c_str(), head, "/")) != 0) {
              return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] list_bucket returns error(%d).")
                          % get_resource_name(_ctx.prop_map()).c_str() % result));
            }

            if (head.IsEmpty()) {
                return SUCCESS();
            }

            // Send stats caching.
            std::string strpath = path;
            if (strcmp(path.c_str(), "/") != 0) {
                strpath += "/";
            }

            s3obj_list_t objects;
            head.GetNameList(objects);

            for (auto& object : objects) {
                DirectoryListStreamManager::get()->add_entry(key, object);
            }
        }


        std::string next_entry;
        if (DirectoryListStreamManager::get()->get_next_entry(key, next_entry)) {

           std::string object_key = key + "/" + next_entry;
           struct stat st;
           headers_t meta;
           if (0 != (result = get_object_attribute(object_key.c_str(), &st, &meta, true, NULL, true))) {
               return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] get_object_attribute on %s returns error(%d).")
                           % get_resource_name(_ctx.prop_map()).c_str() % object_key.c_str() % result));
           }
           *_dirent_ptr = ( rodsDirent_t* ) malloc( sizeof( rodsDirent_t ) );
           boost::filesystem::path p(object_key.c_str());
           strcpy((*_dirent_ptr)->d_name, p.filename().string().c_str());

        }

        return SUCCESS();

    } // s3FileReaddirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX rename
    irods::error s3FileRenamePlugin( irods::plugin_context& _ctx,
                                     const char*         _new_file_name )
    {

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            return PASS(ret);
        }

        ret = set_s3_configuration_from_context(_ctx.prop_map());
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string from = fco->physical_path();

        std::string bucket;
        std::string from_key;
        ret = parseS3Path(from, bucket, from_key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        strncpy(::bucket, bucket.c_str(), MAX_NAME_LEN-1);
        from_key = "/" + from_key;

        std::string new_file_key;
        ret = parseS3Path(_new_file_name, bucket, new_file_key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }
        new_file_key = "/" + new_file_key;

        // TODO S3_RENAME_ERR

        struct stat buf;
        int result;

        S3FS_PRN_DBG("[from=%s][to=%s]", from_key.c_str(), new_file_key.c_str());

        ret = s3FileStatPlugin(_ctx, &buf);
        if(!ret.ok()) {
            return ERROR(S3_FILE_STAT_ERR, boost::str(boost::format("[resource_name=%s] Failed to stat file (%s) during move to (%s)")
                        % get_resource_name(_ctx.prop_map()).c_str() % from.c_str() % _new_file_name));
        }

        // files larger than 5GB must be modified via the multipart interface
        if(!nomultipart && ((long long)buf.st_size >= (long long)FIVE_GB)) {
            result = rename_large_object(from_key.c_str(), new_file_key.c_str());
        } else {
            if(!nocopyapi && !norenameapi){
                result = rename_object(from_key.c_str(), new_file_key.c_str());
            } else {
                result = rename_object_nocopy(from_key.c_str(), new_file_key.c_str());
            }
        }
        S3FS_MALLOCTRIM(0);

        if (result != 0) {
            return ERROR(S3_FILE_COPY_ERR, boost::str(boost::format("[resource_name=%s] Failed to rename file from (%s) to (%s) result = %d")
                        % get_resource_name(_ctx.prop_map()).c_str() % from.c_str() % _new_file_name % result));
        }

        // issue 1855 (irods issue 4326) - resources must now set physical path
        fco->physical_path(_new_file_name);

        return SUCCESS();

    } // s3FileRenamePlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX truncate
    irods::error s3FileTruncatePlugin(
        irods::plugin_context& _ctx )
    {
        return SUCCESS();
    } // s3FileTruncatePlugin


    // interface to determine free space on a device given a path
    irods::error s3FileGetFsFreeSpacePlugin(
        irods::plugin_context& _ctx )
    {
        return SUCCESS();
    } // s3FileGetFsFreeSpacePlugin

    // =-=-=-=-=-=-=-
    // s3StageToCache - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from filename to cacheFilename. optionalInfo info
    // is not used.
    irods::error s3StageToCachePlugin(
        irods::plugin_context& _ctx,
        const char*                               _cache_file_name )
    {
        return ERROR(SYS_NOT_SUPPORTED, boost::str(boost::format("[resource_name=%s] %s") % get_resource_name(_ctx.prop_map()) % __FUNCTION__));
    }

    // =-=-=-=-=-=-=-
    // s3SyncToArch - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from cacheFilename to filename. optionalInfo info
    // is not used.
    irods::error s3SyncToArchPlugin(
        irods::plugin_context& _ctx,
        const char* _cache_file_name )
    {
        return ERROR(SYS_NOT_SUPPORTED, boost::str(boost::format("[resource_name=%s] %s") % get_resource_name(_ctx.prop_map()) % __FUNCTION__));
    }

    // =-=-=-=-=-=-=-
    // redirect_open - code to determine redirection for open operation
    irods::error s3RedirectOpen(
        irods::plugin_property_map&   _prop_map,
        irods::file_object_ptr        _file_obj,
        const std::string&             _resc_name,
        const std::string&             _curr_host,
        float&                         _out_vote ) {


        irods::error result = SUCCESS();


        // =-=-=-=-=-=-=-
        // initially set a good default
        _out_vote = 0.0;

        // =-=-=-=-=-=-=-
        // determine if the resource is down
        int resc_status = 0;
        irods::error get_ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
        if ( ( result = ASSERT_PASS( get_ret, boost::str(boost::format("[resource_name=%s] Failed to get \"status\" property.") % _resc_name.c_str() ) ) ).ok() ) {

            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if ( INT_RESC_STATUS_DOWN != resc_status ) {

                // =-=-=-=-=-=-=-
                // get the resource host for comparison to curr host
                std::string host_name;
                get_ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
                if ( ( result = ASSERT_PASS( get_ret, boost::str(boost::format("[resource_name=%s] Failed to get \"location\" property.") % _resc_name.c_str() ) ) ).ok() ) {

                    // =-=-=-=-=-=-=-
                    // set a flag to test if were at the curr host, if so we vote higher
                    bool curr_host = ( _curr_host == host_name );

                    // =-=-=-=-=-=-=-
                    // make some flags to clarify decision making
                    bool need_repl = ( _file_obj->repl_requested() > -1 );

                    // =-=-=-=-=-=-=-
                    // set up variables for iteration
                    irods::error final_ret = SUCCESS();
                    std::vector< irods::physical_object > objs = _file_obj->replicas();
                    std::vector< irods::physical_object >::iterator itr = objs.begin();

                    // =-=-=-=-=-=-=-
                    // check to see if the replica is in this resource, if one is requested
                    for ( ; itr != objs.end(); ++itr ) {
                        // =-=-=-=-=-=-=-
                        // run the hier string through the parser and get the last
                        // entry.
                        std::string last_resc;
                        irods::hierarchy_parser parser;
                        parser.set_string( itr->resc_hier() );
                        parser.last_resc( last_resc );

                        // =-=-=-=-=-=-=-
                        // more flags to simplify decision making
                        bool repl_us  = ( _file_obj->repl_requested() == itr->repl_num() );
                        bool resc_us  = ( _resc_name == last_resc );
                        bool is_dirty = ( itr->is_dirty() != 1 );

                        // =-=-=-=-=-=-=-
                        // success - correct resource and don't need a specific
                        //           replication, or the repl nums match
                        if ( resc_us ) {
                            // =-=-=-=-=-=-=-
                            // if a specific replica is requested then we
                            // ignore all other criteria
                            if ( need_repl ) {
                                if ( repl_us ) {
                                    _out_vote = 1.0;
                                }
                                else {
                                    // =-=-=-=-=-=-=-
                                    // repl requested and we are not it, vote
                                    // very low
                                    _out_vote = 0.25;
                                }
                            }
                            else {
                                // =-=-=-=-=-=-=-
                                // if no repl is requested consider dirty flag
                                if ( is_dirty ) {
                                    // =-=-=-=-=-=-=-
                                    // repl is dirty, vote very low
                                    _out_vote = 0.25;
                                }
                                else {
                                    // =-=-=-=-=-=-=-
                                    // if our repl is not dirty then a local copy
                                    // wins, otherwise vote middle of the road
                                    if ( curr_host ) {
                                        _out_vote = 1.0;
                                    }
                                    else {
                                        _out_vote = 0.5;
                                    }
                                }
                            }

                            rodsLog(
                                LOG_DEBUG,
                                "open :: resc name [%s] curr host [%s] resc host [%s] vote [%f]",
                                _resc_name.c_str(),
                                _curr_host.c_str(),
                                host_name.c_str(),
                                _out_vote );

                            break;

                        } // if resc_us

                    } // for itr
                }
            }
            else {
                result.code( SYS_RESC_IS_DOWN );
                std::stringstream msg;
                msg << "[resource_name=" << get_resource_name(_prop_map) << "] resource is down";
                return PASSMSG(msg.str(), result);
            }
        }

        return result;

    } // S3RedirectOpen

    // =-=-=-=-=-=-=-
    // used to allow the resource to determine which host
    // should provide the requested operation
    irods::error s3RedirectPlugin(
        irods::plugin_context& _ctx,
        const std::string*                  _opr,
        const std::string*                  _curr_host,
        irods::hierarchy_parser*           _out_parser,
        float*                              _out_vote )
    {
        irods::error result = SUCCESS();
        irods::error ret;

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        printf("%s:%d (%s) [[%lu]] opr=%s\n", __FILE__, __LINE__, __FUNCTION__, thread_id, _opr->c_str());


        // fix open mode so that multipart uploads will work
        if (irods::WRITE_OPERATION == (*_opr)) {
            overwrite_open_mode = O_CREAT | O_WRONLY | O_TRUNC;
        } else {
            overwrite_open_mode = 0;
        }

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // read the data size and save and save it under the property plugin property map
        uint64_t data_size = 0;
        char *data_size_str = getValByKey(&file_obj->cond_input(), DATA_SIZE_KW);
        if (data_size_str) {
            try {
                data_size = boost::lexical_cast<uint64_t>(data_size_str);
            } catch (boost::bad_lexical_cast const& e) {
                data_size = 0;
            }
        }

        _ctx.prop_map().set<uint64_t>(DATA_SIZE_KW, data_size);

        // =-=-=-=-=-=-=-
        // check the context validity
        ret = _ctx.valid< irods::file_object >();
        if ( ( result = ASSERT_PASS( ret, "[resource_name=%s] Invalid resource context.", get_resource_name(_ctx.prop_map()).c_str() ) ).ok() ) {

            // =-=-=-=-=-=-=-
            // check incoming parameters
            if( ( result = ASSERT_ERROR( _opr && _curr_host && _out_parser && _out_vote, SYS_INVALID_INPUT_PARAM,
                                      "[resource_name=%s] One or more NULL pointer arguments.", get_resource_name(_ctx.prop_map()).c_str() ) ).ok() ) {

                std::string resc_name;

                // =-=-=-=-=-=-=-
                // cast down the chain to our understood object type
                irods::file_object_ptr file_obj = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );

                // =-=-=-=-=-=-=-
                // get the name of this resource
                ret = _ctx.prop_map().get< std::string >( irods::RESOURCE_NAME, resc_name );
                if((result = ASSERT_PASS(ret, "Failed to get resource name property.")).ok() ) {

                    // if we are in detached mode, set the location to current host
                    bool attached_mode, cacheless_mode;
                    get_modes_from_properties(_ctx.prop_map(), attached_mode, cacheless_mode);

                    if (!attached_mode && _curr_host) {

                        // set the hostname to the local host
                        _ctx.prop_map().set<std::string>(irods::RESOURCE_LOCATION, *_curr_host);

                        rodsServerHost_t* host = nullptr;
                        rodsLong_t resc_id = 0;

                        ret = _ctx.prop_map().get<rodsLong_t>( irods::RESOURCE_ID, resc_id );
                        if ( !ret.ok() ) {
                            std::string msg = boost::str(boost::format("[resource_name=%s] get_property in s3RedirectPlugin failed to get irods::RESOURCE _ID") % resc_name.c_str() );
                            return PASSMSG( msg, ret );
                        }

                        ret = irods::get_resource_property< rodsServerHost_t* >( resc_id, irods::RESOURCE_HOST, host );
                        if ( !ret.ok() ) {
                            std::string msg = boost::str(boost::format("[resource_name=%s] get_resource_property (irods::RESOURCE_HOST) in s3RedirectPlugin for detached mode failed") % resc_name.c_str() );
                            return PASSMSG( msg, ret );
                        }

                        // pave over host->hostName->name in rodsServerHost_t
                        free(host->hostName->name);
                        host->hostName->name = static_cast<char*>(malloc(strlen(_curr_host->c_str()) + 1));
                        strcpy(host->hostName->name, _curr_host->c_str());
                        host->localFlag = LOCAL_HOST;

                        ret = irods::set_resource_property< rodsServerHost_t* >( resc_name, irods::RESOURCE_HOST, host );
                        if ( !ret.ok() ) {
                            std::string msg = boost::str(boost::format("[resource_name=%s] set_resource_property (irods::RESOURCE_HOST) in s3RedirectPlugin for detached mode failed") % resc_name.c_str() );
                            return PASSMSG( msg, ret );
                        }

                    }


                    // =-=-=-=-=-=-=-
                    // add ourselves to the hierarchy parser by default
                    _out_parser->add_child( resc_name );

                    // =-=-=-=-=-=-=-
                    // test the operation to determine which choices to make
                    if( irods::OPEN_OPERATION == (*_opr) ||
                            irods::WRITE_OPERATION == (*_opr) ||
                            irods::UNLINK_OPERATION == (*_opr) ) {
                        // =-=-=-=-=-=-=-
                        // call redirect determination for 'get' operation
                        result = irods_s3_cacheless::s3RedirectOpen(
                                     _ctx.prop_map(),
                                     file_obj,
                                     resc_name,
                                     (*_curr_host),
                                     (*_out_vote));
                    } else if( irods::CREATE_OPERATION == (*_opr) ) {
                        // =-=-=-=-=-=-=-
                        // call redirect determination for 'create' operation
                        result = s3RedirectCreate( _ctx.prop_map(), *file_obj, resc_name, (*_curr_host), (*_out_vote)  );
                    }
                    else {
                        result = ASSERT_ERROR(false, SYS_INVALID_INPUT_PARAM,
                                      "[resource_name=%s] Unknown redirect operation: \"%s\".", get_resource_name(_ctx.prop_map()).c_str(), _opr->c_str() );
                    }
                }
            }
        }

        return result;
    } // s3RedirectPlugin

    // =-=-=-=-=-=-=-
    // code which would rebalance the resource, S3 does not rebalance.
    irods::error s3FileRebalance(
        irods::plugin_context& _ctx ) {
        return SUCCESS();

    } // s3FileRebalance

    irods::error s3FileNotifyPlugin( irods::plugin_context& _ctx,
        const std::string* str ) {
        return SUCCESS();
    } // s3FileNotifyPlugin

}
