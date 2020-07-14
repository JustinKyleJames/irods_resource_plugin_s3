// =-=-=-=-=-=-=-
// local includes
#include "s3_archive_operations.hpp"
#include "libirods_s3.hpp"
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
#include <irods_hierarchy_parser.hpp>
#include <irods_virtual_path.hpp>
#include <irods_query.hpp>
#include "voting.hpp"

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
#include <list>
#include <map>

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

        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]] call_from=%s oflag=%d oflag&O_TRUNC=%d oflag&O_CREAT=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, call_from.c_str(), oflag, oflag & O_TRUNC, oflag & O_CREAT);
        rodsLog(LOG_DEBUG, "%s:%d (%s) O_WRONLY=%d, O_RDWR=%d, O_RDONLY=%d, O_TRUNC=%d, O_CREAT=%d, O_APPEND=%d\n", __FILE__, __LINE__, __FUNCTION__,
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

    std::string get_protocol_as_string(irods::plugin_property_map& _prop_map)
    {
        std::string proto_str;
        irods::error ret = _prop_map.get< std::string >(s3_proto, proto_str );
        if (!ret.ok()) { // Default to original behavior
            return "https";
        }
        return proto_str;
    }

    std::string get_signature_version_as_string(irods::plugin_property_map& _prop_map)
    {
        std::string version_str;

        irods::error ret = _prop_map.get< std::string >(s3_signature_version, version_str);
        if (ret.ok()) {
            if (version_str == "4" || boost::iequals(version_str, "V4")) {
                return "v4";
            }
        }

        return "v2";
    }

    std::tuple<irods::error, std::shared_ptr<dstream>, std::shared_ptr<s3_transport>> make_dstream(
            irods::plugin_context& _ctx,
            const std::string& call_from)
    {

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // get the file descriptor
        int fd = file_obj->file_descriptor();

        std::shared_ptr<dstream> dstream_ptr;
        std::shared_ptr<s3_transport> s3_transport_ptr;

        // if already done just return
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            if (file_descriptor_to_dstream_map.find(fd) != file_descriptor_to_dstream_map.end()) {
                return make_tuple(SUCCESS(), file_descriptor_to_dstream_map[fd], file_descriptor_to_transport_map[fd]);
            }
        }

        std::string bucket_name;
        std::string object_key;
        std::string access_key;
        std::string secret_access_key;

        irods::error ret = parseS3Path(file_obj->physical_path(), bucket_name, object_key, _ctx.prop_map());
        if(!ret.ok()) {
            return std::make_tuple(PASS(ret), dstream_ptr, s3_transport_ptr);
        }

        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]] [physical_path=%s][bucket_name=%s]\n", __FILE__, __LINE__, __FUNCTION__, thread_id, file_obj->physical_path().c_str(), bucket_name.c_str());

        ret = s3GetAuthCredentials(_ctx.prop_map(), access_key, secret_access_key);
        if(!ret.ok()) {
            return std::make_tuple(PASS(ret), dstream_ptr, s3_transport_ptr);
        }

        // get open mode
        std::ios_base::openmode open_mode;
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            open_mode = fd_to_open_mode_map[fd];
        }

        // get the file size
        // TODO on cp data_size_kw not set
        uint64_t data_size = s3_transport_config::UNKNOWN_OBJECT_SIZE;
rodsLog(LOG_NOTICE, "%s:%d (%s) data_size=%lu\n", __FILE__, __LINE__, __FUNCTION__, data_size);
        /*ret = _ctx.prop_map().get< uint64_t >(DATA_SIZE_KW, data_size);
        if (!ret.ok()) {
            //rodsLog(LOG_ERROR, "%s:%d (%s) [[%lu]] DATA_SIZE_KW is not set\n", __FILE__, __LINE__, __FUNCTION__, thread_id);
            data_size = s3_transport_config::UNKNOWN_OBJECT_SIZE;
        }*/

        // get number of threads
        int requested_number_of_threads = 0;
        for (int i = 0; i < NUM_L1_DESC; ++i) {
           if (L1desc[i].inuseFlag && L1desc[i].dataObjInp->objPath == file_obj->logical_path()) {
               requested_number_of_threads = L1desc[i].dataObjInp->numThreads;

               // if data_size is zero or UNKNOWN, try to get it from L1desc
               if (data_size == s3_transport_config::UNKNOWN_OBJECT_SIZE) {
                   data_size = L1desc[i].dataSize;
rodsLog(LOG_NOTICE, "%s:%d (%s) L1desc[i].data_size=%lu\n", __FILE__, __LINE__, __FUNCTION__, L1desc[i].dataSize);
               }
           }
        }
        _ctx.prop_map().set<uint64_t>(DATA_SIZE_KW, data_size);

        int number_of_threads = getNumThreads( _ctx.comm(),
                data_size,
                requested_number_of_threads,
                const_cast<KeyValPair*>(&file_obj->cond_input()),
                nullptr,                     // destination resc hier
                nullptr,                     // source resc hier
                0 );                         // opr type - not used

        if (number_of_threads < 1) {
            number_of_threads = 1;
        }

        // read the size of the circular buffer from configuration
        unsigned int circular_buffer_size = S3_DEFAULT_CIRCULAR_BUFFER_SIZE;
        std::string circular_buffer_size_str;
        ret = _ctx.prop_map().get<std::string>(s3_circular_buffer_size, circular_buffer_size_str);
        if (ret.ok()) {
            try {
                circular_buffer_size = boost::lexical_cast<unsigned int>(circular_buffer_size_str);
            } catch (boost::bad_lexical_cast &) {}
        }


        std::string&& hostname = s3GetHostname(_ctx.prop_map());

        s3_transport_config s3_config;
        s3_config.hostname = hostname;
        s3_config.object_size = data_size;
        s3_config.number_of_transfer_threads = 20;
        s3_config.part_size = data_size == s3_transport_config::UNKNOWN_OBJECT_SIZE ? 0 : data_size / number_of_threads;
        s3_config.bucket_name = bucket_name;
        s3_config.access_key = access_key;
        s3_config.secret_access_key = secret_access_key;
        s3_config.multipart_upload_flag = number_of_threads > 1;
        s3_config.shared_memory_timeout_in_seconds = 60;
        s3_config.circular_buffer_size = circular_buffer_size;
        s3_config.s3_signature_version_str = get_signature_version_as_string(_ctx.prop_map());
        s3_config.s3_protocol_str = get_protocol_as_string(_ctx.prop_map());
        s3_config.s3_uri_request_style = s3_get_uri_request_style(_ctx.prop_map()) == S3UriStyleVirtualHost ? "host" : "path";
        s3_config.minimum_part_size = s3_get_minimum_part_size(_ctx.prop_map());

        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            s3_transport_ptr = std::make_shared<s3_transport>(s3_config);
            file_descriptor_to_transport_map[fd] = s3_transport_ptr;
            dstream_ptr = std::make_shared<dstream>(*s3_transport_ptr, object_key, open_mode);
            file_descriptor_to_dstream_map[fd] = dstream_ptr;
        }

        /*if (!dstream_ptr->is_open()) {
            return ERROR(S3_FILE_OPEN_ERR, "Failed to create dstream.");
        }*/

        return std::make_tuple(SUCCESS(), dstream_ptr, s3_transport_ptr);
    }

    // =-=-=-=-=-=-=-
    // interface for file registration
    irods::error s3_registered_operation( irods::plugin_context& _ctx) {
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for file unregistration
    irods::error s3_unregistered_operation( irods::plugin_context& _ctx) {
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for file modification
    irods::error s3_modified_operation( irods::plugin_context& _ctx) {
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX create

    irods::error s3_file_create_operation( irods::plugin_context& _ctx) {

        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]]\n", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()));

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

        std::ios_base::openmode open_mode = translate_open_mode_posix_to_stream(O_CREAT | O_WRONLY, __FUNCTION__);

        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            int fd = fd_counter++;
            file_obj->file_descriptor(fd);
            fd_to_open_mode_map[fd] = open_mode;
            rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]] fd=%d open_mode=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, fd, file_obj->flags());
        }

        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    irods::error s3_file_open_operation( irods::plugin_context& _ctx) {

        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]]\n", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()));

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

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
            rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]] fd=%d open_mode=%d overwrite_open_mode=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, fd, file_obj->flags(), overwrite_open_mode);
        }

        return SUCCESS();

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    irods::error s3_file_read_operation( irods::plugin_context& _ctx,
                                   void*               _buf,
                                   int                 _len ) {

        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]]\n", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()));
        irods::error result = SUCCESS();

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        std::shared_ptr<dstream> dstream_ptr;
        std::shared_ptr<s3_transport> s3_transport_ptr;

        std::tie(result, dstream_ptr, s3_transport_ptr) = make_dstream(_ctx, __FUNCTION__);

        if (!result.ok()) {
            return PASS(result);
        }

        if (!dstream_ptr || !dstream_ptr->is_open()) {
            std::stringstream message;
            message << "No valid dstream found.  dstream_ptr=" << static_cast<void*>(dstream_ptr.get());
            return ERROR(S3_FILE_OPEN_ERR, message.str().c_str());
        }


        off_t offset = dstream_ptr->tellg();

        dstream_ptr->read(static_cast<char*>(_buf), _len);
        off_t offset2 = dstream_ptr->tellg();
        int diff = static_cast<int>(offset2 - offset);

        result.code(diff);
        //result.code(_len);
        return result;

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    irods::error s3_file_write_operation( irods::plugin_context& _ctx,
                                    void*               _buf,
                                    int                 _len ) {

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]]\n", __FILE__, __LINE__, __FUNCTION__, thread_id);

        irods::error result = SUCCESS();

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
        int fd = file_obj->file_descriptor();

        // make and read dstream_ptr
        std::shared_ptr<dstream> dstream_ptr;
        std::shared_ptr<s3_transport> s3_transport_ptr;

        std::tie(result, dstream_ptr, s3_transport_ptr) = make_dstream(_ctx, __FUNCTION__);

        if (!result.ok()) {
            return PASS(result);
        }

        if (!dstream_ptr || !dstream_ptr->is_open()) {
            return ERROR(S3_FILE_OPEN_ERR, "No valid dstream found.");
        }

        std::stringstream msg;

        if (!s3_transport_ptr) {
            msg << "No valid transport found for fd " << fd;
            return ERROR(S3_FILE_OPEN_ERR, msg.str());
        }


        uint64_t data_size = 0;
        irods::error ret = _ctx.prop_map().get< uint64_t >(DATA_SIZE_KW, data_size);
        if (!ret.ok()) {
            data_size = 0;
        }

        int requested_number_of_threads = 0;
        for (int i = 0; i < NUM_L1_DESC; ++i) {
           if (L1desc[i].inuseFlag && L1desc[i].dataObjInp->objPath == file_obj->logical_path()) {
               requested_number_of_threads = L1desc[i].dataObjInp->numThreads;
           }
        }

        int number_of_threads = getNumThreads( _ctx.comm(),
                data_size,
                requested_number_of_threads,
                const_cast<KeyValPair*>(&file_obj->cond_input()),
                nullptr,                     // destination resc hier
                nullptr,                     // source resc hier
                0 );                         // opr type - not used

        if (number_of_threads < 1) {
            number_of_threads = 1;
        }

        // determine the part size based on the offset
        off_t offset = dstream_ptr->tellg();
        uint64_t part_size = data_size / number_of_threads;
        if (static_cast<uint64_t>(offset) >= part_size * (number_of_threads-1)) {
            part_size += data_size % number_of_threads;
        }

        s3_transport_ptr->set_part_size(part_size);

        dstream_ptr->write(static_cast<char*>(_buf), _len);
        //off_t after_offset = dstream_ptr->tellg();

        //result.code(after_offset - offset);
        result.code(_len);
        return result;

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error s3_file_close_operation( irods::plugin_context& _ctx ) {

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        int fd = file_obj->file_descriptor();
        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]] fd=%d\n", __FILE__, __LINE__, __FUNCTION__, thread_id, fd);

        if (fd == 0) {
            return SUCCESS();
        }

        std::shared_ptr<dstream> dstream_ptr;

        // if dstream doesn't exist just return
        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            fd_to_open_mode_map.erase(fd);
            if (file_descriptor_to_dstream_map.end() == file_descriptor_to_dstream_map.find(fd)) {
                return SUCCESS();
            }

            dstream_ptr = file_descriptor_to_dstream_map[fd];
        }

        if (dstream_ptr && dstream_ptr->is_open()) {
            dstream_ptr->close();
        }

        {
            std::lock_guard lock(s3_cacheless_plugin_mutex);
            file_descriptor_to_dstream_map.erase(fd);
            dstream_ptr.reset();  // make sure dstream is destructed first
            file_descriptor_to_transport_map.erase(fd);
            if (file_descriptor_to_dstream_map.size() == 0) {
                overwrite_open_mode = 0;
            }
        }

        return SUCCESS();
    }


    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    irods::error s3_file_unlink_operation(
        irods::plugin_context& _ctx) {

rodsLog(LOG_NOTICE, "%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            return PASS(ret);
        }

        size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
        _ctx.prop_map().get<size_t>(s3_retry_count_size_t, retry_count_limit);

        size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;
        _ctx.prop_map().get<size_t>(s3_wait_time_sec_size_t, retry_wait);

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        std::string repl_policy;
        ret = _ctx.prop_map().get<std::string>(
                  REPL_POLICY_KEY,
                  repl_policy);

        // If the policy is set then determine if we should
        // actually unlink the S3 object or not.  If several
        // iRODS replicas point at the same S3 object we only
        // need to unlink in S3 if we are the last S3 registration
        if(ret.ok() && REPL_POLICY_VAL == repl_policy) {
            try {
                std::string vault_path;
                ret = _ctx.prop_map().get<std::string>(
                          irods::RESOURCE_PATH,
                          vault_path);
                if(!ret.ok()) {
                    std::stringstream msg;
                    msg << "[resource_name=" << get_resource_name(_ctx.prop_map()) << "] " << ret.result();
                    return PASSMSG(msg.str(), ret);
                }

                if(!determine_unlink_for_repl_policy(
                        _ctx.comm(),
                        file_obj->logical_path(),
                        vault_path)) {
                        return SUCCESS();
                }
            }
            catch(const irods::exception& _e) {
                return ERROR(
                            _e.code(),
                            _e.what());
            }
        } // if repl_policy

        std::string bucket;
        std::string key;
        ret = parseS3Path(file_obj->physical_path(), bucket, key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        ret = s3InitPerOperation(_ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        std::string key_id;
        std::string access_key;
        ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
        if(!ret.ok()) {
            return PASS(ret);
        }

        S3BucketContext bucketContext = {};
        bucketContext.bucketName = bucket.c_str();
        bucketContext.protocol = s3GetProto(_ctx.prop_map());
        bucketContext.stsDate = s3GetSTSDate(_ctx.prop_map());
        bucketContext.uriStyle = s3_get_uri_request_style(_ctx.prop_map());
        bucketContext.accessKeyId = key_id.c_str();
        bucketContext.secretAccessKey = access_key.c_str();

        callback_data_t data;
        S3ResponseHandler responseHandler = { 0, &responseCompleteCallback };
        size_t retry_cnt = 0;
        do {
            data = {};
            std::string&& hostname = s3GetHostname(_ctx.prop_map());
            bucketContext.hostName = hostname.c_str();
            data.pCtx = &bucketContext;
            S3_delete_object(
                &bucketContext,
                key.c_str(), 0,
                &responseHandler,
                &data);
            if(data.status != S3StatusOK) {
                s3_sleep( retry_wait, 0 );
            }

        } while((data.status != S3StatusOK) &&
                S3_status_is_retryable(data.status) &&
                (++retry_cnt < retry_count_limit));

        if(data.status != S3StatusOK) {
            std::stringstream msg;
            msg << "[resource_name=" << get_resource_name(_ctx.prop_map()) << "] ";
            msg << " - Error unlinking the S3 object: \"";
            msg << file_obj->physical_path();
            msg << "\"";
            if(data.status >= 0) {
                msg << " - \"";
                msg << S3_get_status_name((S3Status)data.status);
                msg << "\"";
            }
            return ERROR(S3_FILE_UNLINK_ERR, msg.str());
        }

        return SUCCESS();

    } // s3_file_unlink_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX Stat
    irods::error s3_file_stat_operation(
        irods::plugin_context& _ctx,
        struct stat* _statbuf )
    {
        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]]\n", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()));

        irods::error result = SUCCESS();

        size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
        _ctx.prop_map().get<size_t>(s3_retry_count_size_t, retry_count_limit);

        size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;
        _ctx.prop_map().get<size_t>(s3_wait_time_sec_size_t, retry_wait);

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if((result = ASSERT_PASS(ret, "[resource_name=%s] Invalid parameters or physical path.", get_resource_name(_ctx.prop_map()).c_str())).ok()) {

            // =-=-=-=-=-=-=-
            // get ref to fco
            irods::data_object_ptr object = boost::dynamic_pointer_cast<irods::data_object>(_ctx.fco());

            bzero (_statbuf, sizeof (struct stat));

            boost::filesystem::path p(object->physical_path());
            std::string filename = p.filename().string();

            irods::error ret;
            std::string bucket;
            std::string key;
            std::string key_id;
            std::string access_key;

            ret = parseS3Path(object->physical_path(), bucket, key, _ctx.prop_map());
            if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed parsing the S3 bucket and key from the physical path: \"%s\".", get_resource_name(_ctx.prop_map()).c_str(),
                                     object->physical_path().c_str())).ok()) {

                ret = s3InitPerOperation( _ctx.prop_map() );
                if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to initialize the S3 system.", get_resource_name(_ctx.prop_map()).c_str())).ok()) {

                    ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                    if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to get the S3 credentials properties.", get_resource_name(_ctx.prop_map()).c_str())).ok()) {

                        callback_data_t data;
                        S3BucketContext bucketContext;// = {};
                        bzero(&bucketContext, sizeof(bucketContext));

                        bucketContext.bucketName = bucket.c_str();
                        bucketContext.protocol = s3GetProto(_ctx.prop_map());
                        bucketContext.stsDate = s3GetSTSDate(_ctx.prop_map());
                        bucketContext.uriStyle = s3_get_uri_request_style(_ctx.prop_map());
                        bucketContext.accessKeyId = key_id.c_str();
                        bucketContext.secretAccessKey = access_key.c_str();

                        S3ResponseHandler headObjectHandler = { &responsePropertiesCallback, &responseCompleteCallbackIgnoreLogNotFound};
                        size_t retry_cnt = 0;
                        do {
                            bzero (&data, sizeof (data));
                            std::string&& hostname = s3GetHostname(_ctx.prop_map());
                            bucketContext.hostName = hostname.c_str();
                            data.pCtx = &bucketContext;
                            S3_head_object(&bucketContext, key.c_str(), 0, &headObjectHandler, &data);
                            if (data.status != S3StatusOK && data.status != S3StatusHttpErrorNotFound) s3_sleep( retry_wait, 0 );
                        } while ( (data.status != S3StatusOK && data.status != S3StatusHttpErrorNotFound) && S3_status_is_retryable(data.status) && (++retry_cnt < retry_count_limit ) );

                        if (data.status == S3StatusOK) {

                            _statbuf->st_mode = S_IFREG;
                            _statbuf->st_nlink = 1;
                            _statbuf->st_uid = getuid ();
                            _statbuf->st_gid = getgid ();
                            _statbuf->st_atime = _statbuf->st_mtime = _statbuf->st_ctime = savedProperties.lastModified;
                            _statbuf->st_size = savedProperties.contentLength;

                        } else if (data.status == S3StatusHttpErrorNotFound) {

                            // assume this is a collection if the key is not found
                            _statbuf->st_mode = S_IFDIR;

                        } else {

                            std::stringstream msg;
                            msg << "[resource_name=" << get_resource_name(_ctx.prop_map()) << "] ";
                            msg << " - Error stat'ing the S3 object: \"" << object->physical_path() << "\"";
                            if (data.status >= 0) {
                                msg << " - \"" << S3_get_status_name((S3Status)data.status) << "\"";
                            }
                            result = ERROR(S3_FILE_STAT_ERR, msg.str());
                        }
                    }
                }
            }
        }
        if( !result.ok() ) {
            std::stringstream msg;
            msg << "[resource_name=" << get_resource_name(_ctx.prop_map()) << "] "
                << result.result();
            rodsLog(LOG_ERROR, msg.str().c_str());
        }
        return result;
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Fstat
    irods::error s3FileFstatPlugin(  irods::plugin_context& _ctx,
                                     struct stat*           _statbuf ) {
        return SUCCESS();

    } // s3FileFstatPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    irods::error s3_file_lseek_operation(  irods::plugin_context& _ctx,
                                     long long              _offset,
                                     int                    _whence ) {

        unsigned long thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]]\n", __FILE__, __LINE__, __FUNCTION__, thread_id);

        irods::error result = SUCCESS();

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // read fd
        int fd = file_obj->file_descriptor();

        std::shared_ptr<dstream> dstream_ptr;
        std::shared_ptr<s3_transport> s3_transport_ptr;

        std::tie(result, dstream_ptr, s3_transport_ptr) = make_dstream(_ctx, __FUNCTION__);

        if (!result.ok()) {
            return PASS(result);
        }

        if (!dstream_ptr || !dstream_ptr->is_open()) {
            return ERROR(S3_FILE_OPEN_ERR, "No valid dstream found.");
        }

        std::stringstream msg;

        if (!s3_transport_ptr) {
            msg << "No valid transport found for fd " << fd;
            return ERROR(S3_FILE_OPEN_ERR, msg.str());
        }

        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu]] offset=%lld\n", __FILE__, __LINE__, __FUNCTION__, thread_id, _offset);

        std::ios_base::seekdir seek_directive =
            _whence == SEEK_SET ? std::ios_base::beg : (
                    _whence == SEEK_END ? std::ios_base::end : std::ios_base::cur);

        dstream_ptr->seekg(_offset, seek_directive);

        uint64_t pos = dstream_ptr->tellg();
        result.code(pos);

        rodsLog(LOG_DEBUG, "%s:%d (%s) [[%lu] tellg=%lu\n", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()), pos);

        return result;

    } // s3_file_lseek_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3_file_mkdir_operation(  irods::plugin_context& _ctx ) {
        return SUCCESS();

    } // s3_file_mkdir_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3_rmdir_operation(  irods::plugin_context& _ctx ) {
        return SUCCESS();
    } // s3_rmdir_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    irods::error s3_opendir_operation( irods::plugin_context& _ctx ) {
        return SUCCESS();
    } // s3_opendir_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    irods::error s3_closedir_operation( irods::plugin_context& _ctx) {
        return SUCCESS();
    } // s3_closedir_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error s3_readdir_operation( irods::plugin_context& _ctx,
                                      struct rodsDirent**     _dirent_ptr ) {

        struct readdir_callback_data {

            struct query_results {

                query_results()
                    : is_truncated(true)
                    , next_marker("")
                    , status(S3StatusOK)
                    , pCtx(nullptr)
                {}

                bool is_truncated;
                std::list<std::string> returned_objects;
                std::list<std::string> returned_collections;
                std::string next_marker;
                S3Status status;
                S3BucketContext *pCtx; /* To enable more detailed error messages */
            };

            std::map<std::string, query_results> result_map;
            std::string query_string;
        };

        S3ListBucketHandler list_bucket_handler = {
            {
                [] (const S3ResponseProperties *properties, void *callback_data) -> S3Status {
                    return S3StatusOK;
                },
                [] (S3Status status, const S3ErrorDetails *error, void *callback_data) -> void {

                    readdir_callback_data *data = static_cast<readdir_callback_data*>(callback_data);
                    std::string query_string = data->query_string;

                    readdir_callback_data::query_results &results = data->result_map[query_string];
                    StoreAndLogStatus( status, error, __FUNCTION__, results.pCtx, &(results.status) );
                }
            },
            [] (int is_truncated, const char *next_marker, int contents_count,
                    const S3ListBucketContent *contents, int common_prefixes_count,
                    const char **common_prefixes, void *callback_data) -> S3Status {

                readdir_callback_data *data = static_cast<readdir_callback_data*>(callback_data);
                std::string query_string = data->query_string;
                readdir_callback_data::query_results &results = data->result_map[query_string];

                results.is_truncated = is_truncated;
                results.next_marker = (next_marker == nullptr ? "" : next_marker);
                for (int i = 0; i < contents_count; ++i) {
                    results.returned_objects.push_back(contents[i].key);
                }

                for (int i = 0; i < common_prefixes_count; ++i) {
                    // remove trailing slash
                    std::string dir_name(common_prefixes[i]);
                    if('/' == dir_name.back()) {
                        dir_name.pop_back();
                    }
                    results.returned_collections.push_back(dir_name);
                }
                return S3StatusOK;
            }
        };

        irods::error result = SUCCESS();


        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if (!ret.ok()) {
            return PASS(ret);
        }

        irods::collection_object_ptr fco = boost::dynamic_pointer_cast< irods::collection_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        std::string bucket;
        std::string key;
        result = parseS3Path(path, bucket, key, _ctx.prop_map());
        if(!result.ok()) {
            return PASS(result);
        }

        thread_local readdir_callback_data cb_data{ {}, "" };

        // add a trailing slash if it is not there
        std::string search_key = key;
        if('/' != search_key.back()) {
             search_key += "/";
        }
        cb_data.query_string = search_key;

        readdir_callback_data::query_results& data = cb_data.result_map[search_key];

        // see if we need to get more data
        if (data.returned_objects.size() == 0 && data.returned_collections.size() == 0 && data.is_truncated) {

            size_t retry_count_limit = S3_DEFAULT_RETRY_COUNT;
            _ctx.prop_map().get<size_t>(s3_retry_count_size_t, retry_count_limit);

            size_t retry_wait = S3_DEFAULT_RETRY_WAIT_SEC;
            _ctx.prop_map().get<size_t>(s3_wait_time_sec_size_t, retry_wait);

            result = s3InitPerOperation( _ctx.prop_map() );
            if(!result.ok()) {
                return PASS(result);
            }

            std::string key_id, access_key;
            result = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
            if(!result.ok()) {
                return PASS(result);
            }

            S3BucketContext bucketContext = {};

            bucketContext.bucketName = bucket.c_str();
            bucketContext.protocol = s3GetProto(_ctx.prop_map());
            bucketContext.stsDate = s3GetSTSDate(_ctx.prop_map());
            bucketContext.uriStyle = s3_get_uri_request_style(_ctx.prop_map());
            bucketContext.accessKeyId = key_id.c_str();
            bucketContext.secretAccessKey = access_key.c_str();

            size_t retry_cnt = 0;
            do {

                std::string&& hostname = s3GetHostname(_ctx.prop_map());
                bucketContext.hostName = hostname.c_str();
                data.pCtx = &bucketContext;

                S3_list_bucket(&bucketContext,                                       // S3BucketContext
                        search_key.c_str(),                                          // prefix
                        data.next_marker == "" ? nullptr : data.next_marker.c_str(), // marker
                        "/",                                                         // delimiter
                        1024,                                                        // max number returned
                        nullptr,                                                     // S3RequestContext
                        &list_bucket_handler,                                        // S3ListBucketHandler
                        &cb_data                                                        // void* callback data
                        );

                if (data.status != S3StatusOK) s3_sleep( retry_wait, 0 );

            } while ( (data.status != S3StatusOK) && S3_status_is_retryable(data.status) && (++retry_cnt < retry_count_limit ) );

            if (data.status != S3StatusOK) {
                std::stringstream msg;
                msg << "[resource_name=" << get_resource_name(_ctx.prop_map()) << "] ";
                msg << " - Error in S3 listing: \"" << search_key.c_str() << "\"";
                if (data.status >= 0) {
                    msg << " - \"" << S3_get_status_name((S3Status)data.status) << "\"";
                }
                return ERROR(S3_FILE_STAT_ERR, msg.str());
            }
        }

        *_dirent_ptr = nullptr;
        if (data.returned_objects.size() > 0) {

            std::string current_key = data.returned_objects.front();
            data.returned_objects.pop_front();

            // a trailing slash indicates a "directory", remove it
            // for boost::filesystem::path can get dir name then add
            // it back in
            //bool add_trailing_slash = false;
            //if('/' == current_key.back()) {
            //    add_trailing_slash = true;
            //    current_key.pop_back();
            //}

            *_dirent_ptr = ( rodsDirent_t* ) malloc( sizeof( rodsDirent_t ) );
            boost::filesystem::path p(current_key.c_str());
            current_key = p.filename().string();
            //if (add_trailing_slash) {
            //    current_key += "/";
            //}
            strcpy((*_dirent_ptr)->d_name, current_key.c_str());
            return result;
        }

        if (data.returned_collections.size() > 0) {

            std::string current_key = data.returned_collections.front();
            data.returned_collections.pop_front();
            *_dirent_ptr = ( rodsDirent_t* ) malloc( sizeof( rodsDirent_t ) );
            boost::filesystem::path p(current_key.c_str());
            current_key = p.filename().string();
            strcpy((*_dirent_ptr)->d_name, current_key.c_str());
            return result;
        }

        return result;

    } // s3_readdir_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX rename
    irods::error s3_file_rename_operation( irods::plugin_context& _ctx,
                                     const char*         _new_file_name )
    {
        irods::error result = SUCCESS();
        irods::error ret;
        std::string key_id;
        std::string access_key;

        // retrieve archive naming policy from resource plugin context
        std::string archive_naming_policy = CONSISTENT_NAMING; // default
        ret = _ctx.prop_map().get<std::string>(ARCHIVE_NAMING_POLICY_KW, archive_naming_policy); // get plugin context property
        if(!ret.ok()) {

            std::stringstream msg;
            msg << "[resource_name=" << get_resource_name(_ctx.prop_map()) << "] "
                << ret.result();
            rodsLog(LOG_ERROR, msg.str().c_str());
        }
        boost::to_lower(archive_naming_policy);

        irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // if archive naming policy is decoupled we're done
        if (archive_naming_policy == DECOUPLED_NAMING) {
            object->file_descriptor(ENOSYS);
            return SUCCESS();
        }

        ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
        if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to get S3 credential properties.", get_resource_name(_ctx.prop_map()).c_str())).ok()) {

            // copy the file to the new location
            ret = s3CopyFile(_ctx, object->physical_path(), _new_file_name, key_id, access_key,
                             s3GetProto(_ctx.prop_map()), s3GetSTSDate(_ctx.prop_map()), s3_get_uri_request_style(_ctx.prop_map()));
            if((result = ASSERT_PASS(ret, "[resource_name=%s] Failed to copy file from: \"%s\" to \"%s\".", get_resource_name(_ctx.prop_map()).c_str(),
                                     object->physical_path().c_str(), _new_file_name)).ok()) {
                // delete the old file
                ret = s3_file_unlink_operation(_ctx);
                result = ASSERT_PASS(ret, "[resource_name=%s] Failed to unlink old S3 file: \"%s\".", get_resource_name(_ctx.prop_map()).c_str(),
                                     object->physical_path().c_str());
            }
        }

        // issue 1855 (irods issue 4326) - resources must now set physical path
        object->physical_path(_new_file_name);

        return result;
    } // s3_file_rename_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX truncate
    irods::error s3FileTruncatePlugin(
        irods::plugin_context& _ctx )
    {
        return SUCCESS();
    } // s3FileTruncatePlugin


    // interface to determine free space on a device given a path
    irods::error s3_get_fs_freespace_operation(
        irods::plugin_context& _ctx )
    {
        return SUCCESS();
    } // s3_get_fs_freespace_operation

    // =-=-=-=-=-=-=-
    // s3StageToCache - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from filename to cacheFilename. optionalInfo info
    // is not used.
    irods::error s3_stage_to_cache_operation(
        irods::plugin_context& _ctx,
        const char*                               _cache_file_name )
    {
        return ERROR(SYS_NOT_SUPPORTED,
                boost::str(boost::format("[resource_name=%s] %s") %
                    get_resource_name(_ctx.prop_map()) % __FUNCTION__));
    }

    // =-=-=-=-=-=-=-
    // s3SyncToArch - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from cacheFilename to filename. optionalInfo info
    // is not used.
    irods::error s3_sync_to_arch_operation(
        irods::plugin_context& _ctx,
        const char* _cache_file_name )
    {
        return ERROR(SYS_NOT_SUPPORTED,
                boost::str(boost::format("[resource_name=%s] %s") %
                    get_resource_name(_ctx.prop_map()) % __FUNCTION__));
    }

    // =-=-=-=-=-=-=-
    // redirect_open - code to determine redirection for open operation
    irods::error s3_resolve_resc_hier_open(
        irods::plugin_property_map&   _prop_map,
        irods::file_object_ptr        _file_obj,
        const std::string&             _resc_name,
        const std::string&             _curr_host,
        float&                         _out_vote ) {

printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);


        irods::error result = SUCCESS();


        // =-=-=-=-=-=-=-
        // initially set a good default
        _out_vote = 0.0;

        // =-=-=-=-=-=-=-
        // determine if the resource is down
        int resc_status = 0;
        irods::error get_ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
        if ( ( result = ASSERT_PASS( get_ret,
                        boost::str(boost::format("[resource_name=%s] Failed to get \"status\" property.") %
                            _resc_name.c_str() ) ) ).ok() ) {

printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);
            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if ( INT_RESC_STATUS_DOWN != resc_status ) {

printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);
                // =-=-=-=-=-=-=-
                // get the resource host for comparison to curr host
                std::string host_name;
                get_ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
                if ( ( result = ASSERT_PASS( get_ret,
                                boost::str(boost::format("[resource_name=%s] Failed to get \"location\" property.") %
                                    _resc_name.c_str() ) ) ).ok() ) {

printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);
                    // =-=-=-=-=-=-=-
                    // set a flag to test if were at the curr host, if so we vote higher
                    bool curr_host = ( _curr_host == host_name );
printf("%s:%d (%s) host_name=%s curr_host=%d\n", __FILE__, __LINE__, __FUNCTION__, host_name.c_str(), curr_host);

                    // =-=-=-=-=-=-=-
                    // make some flags to clarify decision making
                    bool need_repl = ( _file_obj->repl_requested() > -1 );

                    // =-=-=-=-=-=-=-
                    // set up variables for iteration
                    irods::error final_ret = SUCCESS();
                    std::vector< irods::physical_object > objs = _file_obj->replicas();
                    std::vector< irods::physical_object >::iterator itr = objs.begin();
printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);

                    // =-=-=-=-=-=-=-
                    // check to see if the replica is in this resource, if one is requested
                    for ( ; itr != objs.end(); ++itr ) {
                        // =-=-=-=-=-=-=-
                        // run the hier string through the parser and get the last
                        // entry.
printf("%s:%d (%s)\n", __FILE__, __LINE__, __FUNCTION__);
                        std::string last_resc;
                        irods::hierarchy_parser parser;
printf("%s:%d (%s) resc_hier=%s\n", __FILE__, __LINE__, __FUNCTION__, itr->resc_hier().c_str());
                        parser.set_string( itr->resc_hier() );
                        parser.last_resc( last_resc );

                        // =-=-=-=-=-=-=-
                        // more flags to simplify decision making
                        bool repl_us  = ( _file_obj->repl_requested() == itr->repl_num() );
                        bool resc_us  = ( _resc_name == last_resc );
                        bool is_dirty = ( itr->replica_status() != 1 );

                        // =-=-=-=-=-=-=-
                        // success - correct resource and don't need a specific
                        //           replication, or the repl nums match
                        if ( resc_us ) {
                            // =-=-=-=-=-=-=-
                            // if a specific replica is requested then we
                            // ignore all other criteria
                            if ( need_repl ) {
                                if ( repl_us ) {
printf("%s:%d (%s) vote=1.0\n", __FILE__, __LINE__, __FUNCTION__);
                                    _out_vote = 1.0;
                                }
                                else {
                                    // =-=-=-=-=-=-=-
                                    // repl requested and we are not it, vote
                                    // very low
printf("%s:%d (%s) vote=.25\n", __FILE__, __LINE__, __FUNCTION__);
                                    _out_vote = 0.25;
                                }
                            }
                            else {
                                // =-=-=-=-=-=-=-
                                // if no repl is requested consider dirty flag
                                if ( is_dirty ) {
                                    // =-=-=-=-=-=-=-
                                    // repl is dirty, vote very low
printf("%s:%d (%s) vote=.25\n", __FILE__, __LINE__, __FUNCTION__);
                                    _out_vote = 0.25;
                                }
                                else {
                                    // =-=-=-=-=-=-=-
                                    // if our repl is not dirty then a local copy
                                    // wins, otherwise vote middle of the road
                                    if ( curr_host ) {
printf("%s:%d (%s) vote=1.0\n", __FILE__, __LINE__, __FUNCTION__);
                                        _out_vote = 1.0;
                                    }
                                    else {
printf("%s:%d (%s) vote=.5\n", __FILE__, __LINE__, __FUNCTION__);
                                        _out_vote = 0.5;
                                    }
                                }
                            }

                            rodsLog(
                                LOG_ERROR, // TODO
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
    irods::error s3_resolve_resc_hier_operation(
        irods::plugin_context& _ctx,
        const std::string*                  _opr,
        const std::string*                  _curr_host,
        irods::hierarchy_parser*           _out_parser,
        float*                              _out_vote )
    {
        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // fix open mode so that multipart uploads will work
        if (irods::WRITE_OPERATION == (*_opr) || irods::CREATE_OPERATION == (*_opr)) {
            overwrite_open_mode = O_CREAT | O_WRONLY | O_TRUNC;
        } else {
            overwrite_open_mode = 0;
        }

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

        namespace irv = irods::experimental::resource::voting;

        if (irods::error ret = _ctx.valid<irods::file_object>(); !ret.ok()) {
            return PASSMSG("Invalid resource context.", ret);
        }

        if (!_opr || !_curr_host || !_out_parser || !_out_vote) {
            return ERROR(SYS_INVALID_INPUT_PARAM, "Invalid input parameter.");
        }

        if (getValByKey(&file_obj->cond_input(), RECURSIVE_OPR__KW)) {
            rodsLog(LOG_DEBUG,
                "%s: %s found in cond_input for file_obj",
                __FUNCTION__, RECURSIVE_OPR__KW);
        }

        _out_parser->add_child(irods::get_resource_name(_ctx));
        *_out_vote = irv::vote::zero;
        try {
            *_out_vote = irv::calculate(*_opr, _ctx, *_curr_host, *_out_parser);
            return SUCCESS();
        }
        catch(const std::out_of_range& e) {
            return ERROR(INVALID_OPERATION, e.what());
        }
        catch (const irods::exception& e) {
            return irods::error(e);
        }

        return ERROR(SYS_UNKNOWN_ERROR, "An unknown error occurred while resolving hierarchy.");

    } // s3_resolve_resc_hier_operation

    // =-=-=-=-=-=-=-
    // code which would rebalance the resource, S3 does not rebalance.
    irods::error s3_rebalance_operation(
        irods::plugin_context& _ctx ) {
        return SUCCESS();

    } // s3_rebalance_operation

    irods::error s3_notify_operation( irods::plugin_context& _ctx,
        const std::string* str ) {
        return SUCCESS();
    } // s3_notify_operation

}
