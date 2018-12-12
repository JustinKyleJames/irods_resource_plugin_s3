
// =-=-=-=-=-=-=-
// local includes
#include "s3_archive_operations.hpp"
#include "libirods_s3.hpp"
#include "s3fs/curl.h"
#include "s3fs/cache.h"
#include "s3fs/fdcache.h"
#include "s3fs/s3fs.h"

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

// =-=-=-=-=-=-=-
// boost includes
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

#include <string>

extern size_t g_retry_count;
extern size_t g_retry_wait;

extern S3ResponseProperties savedProperties;

// common function for creation of a plain object
/*static int create_file_object(const char* path)
{
    headers_t meta;
    meta["Content-Type"]     = S3fsCurl::LookupMimeType(std::string(path));
    //meta["x-amz-meta-uid"]   = str(uid);
    //meta["x-amz-meta-gid"]   = str(gid);
    //meta["x-amz-meta-mode"]  = str(mode);
    //meta["x-amz-meta-mtime"] = str(time(NULL));
  
    S3fsCurl s3fscurl(true);
    return s3fscurl.PutRequest(path, meta, -1);    // fd=-1 means for creating zero byte object.
}*/

        
namespace irods_s3_cacheless {

    int create_file_object(std::string& path) 
    {
rodsLog(LOG_ERROR, "%s:%d [path=%s]", __FUNCTION__, __LINE__, path.c_str());

        headers_t meta;
        meta["Content-Type"]     = S3fsCurl::LookupMimeType(path);
        meta["x-amz-meta-uid"]   = "999";
        meta["x-amz-meta-gid"]   = "999";
        meta["x-amz-meta-mode"]  = "33204";
        //meta["x-amz-meta-mtime"] = std::string(time(NULL));

        S3fsCurl s3fscurl(true);
        return s3fscurl.PutRequest(path.c_str(), meta, -1);    // fd=-1 means for creating zero byte object.
    }

    void flush_buffer(std::string& path, int fh) {
        FdEntity* ent;
        if (NULL != (ent = FdManager::get()->ExistOpen(path.c_str(), fh))) {
            //ent->UpdateMtime();
            ent->Flush(false);
            FdManager::get()->Close(ent);
        }
        S3FS_MALLOCTRIM(0);
        return;
    }

    // =-=-=-=-=-=-=-
    // interface for file registration
    irods::error s3RegisteredPlugin( irods::plugin_context& _ctx) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for file unregistration
    irods::error s3UnregisteredPlugin( irods::plugin_context& _ctx) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for file modification
    irods::error s3ModifiedPlugin( irods::plugin_context& _ctx) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX create

    //static int s3fs_create(const char* path, mode_t mode, struct fuse_file_info* fi)
    irods::error s3FileCreatePlugin( irods::plugin_context& _ctx) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);

        service_path = "";

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        //std::string path = s3_get_full_path_from_protocol(fco->physical_path(), _ctx.prop_map()) ;
        std::string path = fco->physical_path();

rodsLog(LOG_ERROR, "%s:%d File create on %s, physical_path=%s", __FUNCTION__, __LINE__, path.c_str(), fco->physical_path().c_str());

        int result;

        std::string bucket;
        std::string key;
        parseS3Path(fco->physical_path(), bucket, key);

        result = create_file_object(path);
        StatCache::getStatCacheData()->DelStat(path.c_str());
        if(result != 0){
          return ERROR(S3_PUT_ERROR, (boost::format("Error in %s.  Code is %d") % __FUNCTION__ % result).str());
        }


        FdEntity*   ent;
        headers_t   meta;
        get_object_attribute(path.c_str(), NULL, &meta, true, NULL, true);    // no truncate cache
        if(NULL == (ent = FdManager::get()->Open(path.c_str(), &meta, 0, -1, false, true))){
          StatCache::getStatCacheData()->DelStat(path.c_str());
          return ERROR(S3_PUT_ERROR, (boost::format("Error in %s.  Code is EIO") % __FUNCTION__));
        }

        fco->file_descriptor(ent->GetFd());
        //fi->fh = ent->GetFd();
        S3FS_MALLOCTRIM(0);

        return SUCCESS();
    }
/*
    irods::error s3FileCreatePlugin_bu( irods::plugin_context& _ctx) {

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

rodsLog(LOG_ERROR, "%s:%d File create on %s", __FUNCTION__, __LINE__, fco->physical_path().c_str());

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<
            irods::file_object>(_ctx.fco());


        int result = create_file_object(path);

        StatCache::getStatCacheData()->DelStat(path);
        if(result != 0){
          return ERROR(S3_PUT_ERROR, boost::format("%s: create_file_object returned [%d]") % __FUNCTION__ % result);
        }
  
        FdEntity*   ent; 
        headers_t   meta;
        get_object_attribute(path, NULL, &meta, true, NULL, true);    // no truncate cache
        if(NULL == (ent = FdManager::get()->Open(path, &meta, 0, -1, false, true))){
          StatCache::getStatCacheData()->DelStat(path);
          return ERROR(S3_PUT_ERROR, __FUNCTION__);
        }
        //fi->fh = ent->GetFd();
        S3FS_MALLOCTRIM(0);

        return SUCCESS();

    }*/

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    irods::error s3FileOpenPlugin( irods::plugin_context& _ctx) {
rodsLog(LOG_ERROR, "%s:%d", __FUNCTION__, __LINE__);
return SUCCESS();

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    irods::error s3FileReadPlugin( irods::plugin_context& _ctx,
                                   void*               _buf,
                                   int                 _len ) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();

        return ERROR( SYS_NOT_SUPPORTED, __FUNCTION__ );

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    // static int s3fs_write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* fi)
    irods::error s3FileWritePlugin( irods::plugin_context& _ctx,
                                    void*               _buf,
                                    int                 _len ) {

rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);

        irods::error result = SUCCESS();

        irods::file_object_ptr fco = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string path = fco->physical_path();

        // TODO is offset always 0?
        off_t offset = 0;
        ssize_t res;

        S3FS_PRN_DBG("[path=%s][size=%zu][offset=%jd][fd=%llu]", path.c_str(), _len, (intmax_t)offset, (unsigned long long)(fco->file_descriptor()));

        FdEntity* ent;
        if(NULL == (ent = FdManager::get()->ExistOpen(path.c_str(), static_cast<int>(fco->file_descriptor())))){
          S3FS_PRN_ERR("could not find opened fd(%s)", path.c_str());
          return ERROR(S3_PUT_ERROR, (boost::format("Error in %s.  Code is EIO") % __FUNCTION__));
        }
        if(ent->GetFd() != static_cast<int>(fco->file_descriptor())){
          S3FS_PRN_WARN("different fd(%d - %llu)", ent->GetFd(), (unsigned long long)(fco->file_descriptor()));
        }

        if(0 > (res = ent->Write(static_cast<const char*>(_buf), offset, _len))){
          S3FS_PRN_WARN("failed to write file(%s). result=%jd", path.c_str(), (intmax_t)res);
        }
        FdManager::get()->Close(ent);

        flush_buffer(path, ent->GetFd());

        result.code(res);
        return result;

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error s3FileClosePlugin(  irods::plugin_context& _ctx ) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
        return SUCCESS();
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    irods::error s3FileUnlinkPlugin(

        irods::plugin_context& _ctx) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();


        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            std::stringstream msg;
            msg << __FUNCTION__ << " - Invalid parameters or physical path.";
            return PASSMSG(msg.str(), ret);
        }

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<
                                              irods::file_object>(
                                                      _ctx.fco());
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
                    return PASS(ret);
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
        ret = parseS3Path(file_obj->physical_path(), bucket, key);
        if(!ret.ok()) {
            return PASS(ret);
        }

        ret = s3Init(_ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        std::string key_id;
        std::string access_key;
        ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
        if(!ret.ok()) {
            return PASS(ret);
        }

        S3BucketContext bucketContext;
        bzero(&bucketContext, sizeof(bucketContext));
        bucketContext.bucketName = bucket.c_str();
        bucketContext.protocol = s3GetProto(_ctx.prop_map());
        bucketContext.stsDate = s3GetSTSDate(_ctx.prop_map());
        bucketContext.uriStyle = S3UriStylePath;
        bucketContext.accessKeyId = key_id.c_str();
        bucketContext.secretAccessKey = access_key.c_str();

        callback_data_t data;
        S3ResponseHandler responseHandler = { 0, &responseCompleteCallback };
        size_t retry_cnt = 0;
        do {
            bzero (&data, sizeof (data));
            bucketContext.hostName = s3GetHostname();
            data.pCtx = &bucketContext;
            S3_delete_object(
                &bucketContext,
                key.c_str(), 0,
                &responseHandler,
                &data);
            if(data.status != S3StatusOK) {
                s3_sleep( g_retry_wait, 0 );
            }

        } while((data.status != S3StatusOK) &&
                S3_status_is_retryable(data.status) &&
                (++retry_cnt < g_retry_count));

        if(data.status != S3StatusOK) {
            std::stringstream msg;
            msg << __FUNCTION__;
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
    } // s3FileUnlinkPlugin 

    // =-=-=-=-=-=-=-
    // interface for POSIX Stat
    irods::error s3FileStatPlugin(
        irods::plugin_context& _ctx,
        struct stat* _statbuf )
    {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);

        irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {

            // =-=-=-=-=-=-=-
            // get ref to fco
            irods::data_object_ptr _object = boost::dynamic_pointer_cast<irods::data_object>(_ctx.fco());

            bzero (_statbuf, sizeof (struct stat));

            if(_object->physical_path().find("/", _object->physical_path().size()) != std::string::npos) {
                // A directory
                _statbuf->st_mode = S_IFDIR;
            } else {

                irods::error ret;
                std::string bucket;
                std::string key;
                std::string key_id;
                std::string access_key;

                ret = parseS3Path(_object->physical_path(), bucket, key);
                if((result = ASSERT_PASS(ret, "Failed parsing the S3 bucket and key from the physical path: \"%s\".",
                                         _object->physical_path().c_str())).ok()) {

                    ret = s3Init( _ctx.prop_map() );
                    if((result = ASSERT_PASS(ret, "Failed to initialize the S3 system.")).ok()) {

                        ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                        if((result = ASSERT_PASS(ret, "Failed to get the S3 credentials properties.")).ok()) {

                            callback_data_t data;
                            S3BucketContext bucketContext;

                            bzero (&bucketContext, sizeof (bucketContext));
                            bucketContext.bucketName = bucket.c_str();
                            bucketContext.protocol = s3GetProto(_ctx.prop_map());
                            bucketContext.stsDate = s3GetSTSDate(_ctx.prop_map());
                            bucketContext.uriStyle = S3UriStylePath;
                            bucketContext.accessKeyId = key_id.c_str();
                            bucketContext.secretAccessKey = access_key.c_str();

                            S3ResponseHandler headObjectHandler = { &responsePropertiesCallback, &responseCompleteCallback };
                            size_t retry_cnt = 0;
                            do {
                                bzero (&data, sizeof (data));
                                bucketContext.hostName = s3GetHostname();
                                data.pCtx = &bucketContext;
                                S3_head_object(&bucketContext, key.c_str(), 0, &headObjectHandler, &data);
                                if (data.status != S3StatusOK) s3_sleep( g_retry_wait, 0 );
                            } while ( (data.status != S3StatusOK) && S3_status_is_retryable(data.status) && (++retry_cnt < g_retry_count) );

                            if (data.status != S3StatusOK) {
                                std::stringstream msg;
                                msg << __FUNCTION__ << " - Error stat'ing the S3 object: \"" << _object->physical_path() << "\"";
                                if (data.status >= 0) {
                                    msg << " - \"" << S3_get_status_name((S3Status)data.status) << "\"";
                                }
                                result = ERROR(S3_FILE_STAT_ERR, msg.str());
                            }

                            else {
                                _statbuf->st_mode = S_IFREG;
                                _statbuf->st_nlink = 1;
                                _statbuf->st_uid = getuid ();
                                _statbuf->st_gid = getgid ();
                                _statbuf->st_atime = _statbuf->st_mtime = _statbuf->st_ctime = savedProperties.lastModified;
                                _statbuf->st_size = savedProperties.contentLength;
                            }
                        }
                    }
                }
            }
        }
        if( !result.ok() ) {
            irods::log( result );
        }
        return result;
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Fstat
    irods::error s3FileFstatPlugin(  irods::plugin_context& _ctx,
                                     struct stat*        _statbuf ) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();

        return ERROR( SYS_NOT_SUPPORTED, "s3FileFstatPlugin" );

    } // s3FileFstatPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    irods::error s3FileLseekPlugin(  irods::plugin_context& _ctx,
                                     size_t              _offset,
                                     int                 _whence ) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();

        return ERROR( SYS_NOT_SUPPORTED, "s3FileLseekPlugin" );

    } // wosFileLseekPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3FileMkdirPlugin(  irods::plugin_context& _ctx ) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();

        return ERROR( SYS_NOT_SUPPORTED, "s3FileMkdirPlugin" );

    } // s3FileMkdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3FileRmdirPlugin(  irods::plugin_context& _ctx ) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();

        return ERROR( SYS_NOT_SUPPORTED, "s3FileRmdirPlugin" );
    } // s3FileRmdirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    irods::error s3FileOpendirPlugin( irods::plugin_context& _ctx ) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();

        return ERROR( SYS_NOT_SUPPORTED, "s3FileOpendirPlugin" );
    } // s3FileOpendirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    irods::error s3FileClosedirPlugin( irods::plugin_context& _ctx) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();

        return ERROR( SYS_NOT_SUPPORTED, "s3FileClosedirPlugin" );
    } // s3FileClosedirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error s3FileReaddirPlugin( irods::plugin_context& _ctx,
                                      struct rodsDirent**     _dirent_ptr ) {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();

        return ERROR( SYS_NOT_SUPPORTED, "s3FileReaddirPlugin" );
    } // s3FileReaddirPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX rename
    irods::error s3FileRenamePlugin( irods::plugin_context& _ctx,
                                     const char*         _new_file_name )
    {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();
        irods::error result = SUCCESS();
        irods::error ret;
        std::string key_id;
        std::string access_key;

        // retrieve archive naming policy from resource plugin context
        std::string archive_naming_policy = CONSISTENT_NAMING; // default
        ret = _ctx.prop_map().get<std::string>(ARCHIVE_NAMING_POLICY_KW, archive_naming_policy); // get plugin context property
        if(!ret.ok()) {
            irods::log(PASS(ret));
        }
        boost::to_lower(archive_naming_policy);

        irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // if archive naming policy is decoupled we're done
        if (archive_naming_policy == DECOUPLED_NAMING) {
            object->file_descriptor(ENOSYS);
            return SUCCESS();
        }

        ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
        if((result = ASSERT_PASS(ret, "Failed to get S3 credential properties.")).ok()) {

            // copy the file to the new location
            ret = s3CopyFile(_ctx, object->physical_path(), _new_file_name, key_id, access_key,
                             s3GetProto(_ctx.prop_map()), s3GetSTSDate(_ctx.prop_map()));
            if((result = ASSERT_PASS(ret, "Failed to copy file from: \"%s\" to \"%s\".",
                                     object->physical_path().c_str(), _new_file_name)).ok()) {
                // delete the old file
                ret = s3FileUnlinkPlugin(_ctx);
                result = ASSERT_PASS(ret, "Failed to unlink old S3 file: \"%s\".",
                                     object->physical_path().c_str());
            }
        }
        return result;
    } // s3FileRenamePlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX truncate
    irods::error s3FileTruncatePlugin(
        irods::plugin_context& _ctx )
    {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();
        return ERROR( SYS_NOT_SUPPORTED, "s3FileTruncatePlugin" );
    } // s3FileTruncatePlugin


    // interface to determine free space on a device given a path
    irods::error s3FileGetFsFreeSpacePlugin(
        irods::plugin_context& _ctx )
    {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();
 
        return ERROR(SYS_NOT_SUPPORTED, "s3FileGetFsFreeSpacePlugin");

    } // s3FileGetFsFreeSpacePlugin

    irods::error s3FileCopyPlugin( int mode, const char *srcFileName,
                                   const char *destFileName)
    {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();
 
        return ERROR( SYS_NOT_SUPPORTED, "s3FileCopyPlugin" );
    }


    // =-=-=-=-=-=-=-
    // s3StageToCache - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from filename to cacheFilename. optionalInfo info
    // is not used.
    irods::error s3StageToCachePlugin(
        irods::plugin_context& _ctx,
        const char*                               _cache_file_name )
    {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
return SUCCESS();
         irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {

            struct stat statbuf;
            std::string key_id;
            std::string access_key;

            irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

            ret = s3FileStatPlugin(_ctx, &statbuf);
            if((result = ASSERT_PASS(ret, "Failed stating the file: \"%s\".",
                                     object->physical_path().c_str())).ok()) {

                if((result = ASSERT_ERROR((statbuf.st_mode & S_IFREG) != 0, S3_FILE_STAT_ERR, "Error stating the file: \"%s\".",
                                          object->physical_path().c_str())).ok()) {

                    if((result = ASSERT_ERROR(object->size() <= 0 || object->size() == static_cast<size_t>(statbuf.st_size), SYS_COPY_LEN_ERR,
                                              "Error for file: \"%s\" inp data size: %ld does not match stat size: %ld.",
                                              object->physical_path().c_str(), object->size(), statbuf.st_size)).ok()) {

                        ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                        if((result = ASSERT_PASS(ret, "Failed to get S3 credential properties.")).ok()) {

                            ret = s3GetFile( _cache_file_name, object->physical_path(), statbuf.st_size, key_id, access_key, _ctx.prop_map());
                            result = ASSERT_PASS(ret, "Failed to copy the S3 object: \"%s\" to the cache: \"%s\".",
                                                 object->physical_path().c_str(), _cache_file_name);
                        }
                    }
                }
            }
        }
        return result;
    } // s3StageToCachePlugin

    // =-=-=-=-=-=-=-
    // s3SyncToArch - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from cacheFilename to filename. optionalInfo info
    // is not used.
    irods::error s3SyncToArchPlugin(
        irods::plugin_context& _ctx,
        const char* _cache_file_name )
    {
rodsLog(LOG_ERROR, "%s:%d ----------------- ", __FUNCTION__, __LINE__);
        irods::error result = SUCCESS();
        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if((result = ASSERT_PASS(ret, "Invalid parameters or physical path.")).ok()) {

            struct stat statbuf;
            int status;
            std::string key_id;
            std::string access_key;

            irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
            status = stat(_cache_file_name, &statbuf);
            int err_status = UNIX_FILE_STAT_ERR - errno;
            if((result = ASSERT_ERROR(status >= 0, err_status, "Failed to stat cache file: \"%s\".",
                                      _cache_file_name)).ok()) {

                if((result = ASSERT_ERROR((statbuf.st_mode & S_IFREG) != 0, UNIX_FILE_STAT_ERR, "Cache file: \"%s\" is not a file.",
                                          _cache_file_name)).ok()) {

                    ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                    if((result = ASSERT_PASS(ret, "Failed to get S3 credential properties.")).ok()) {

                        std::string default_hostname;
                        ret = _ctx.prop_map().get< std::string >(
                            s3_default_hostname,
                            default_hostname );
                        if( !ret.ok() ) {
                            irods::log(ret);
                        }

                        // retrieve archive naming policy from resource plugin context
                        std::string archive_naming_policy = CONSISTENT_NAMING; // default
                        ret = _ctx.prop_map().get<std::string>(ARCHIVE_NAMING_POLICY_KW, archive_naming_policy); // get plugin context property
                        if(!ret.ok()) {
                            irods::log(ret);
                        }
                        boost::to_lower(archive_naming_policy);

                        // if archive naming policy is decoupled
                        // we use the object's reversed id as S3 key name prefix
                        if (archive_naming_policy == DECOUPLED_NAMING) {
                            // extract object name and bucket name from physical path
                            std::vector< std::string > tokens;
                            irods::string_tokenize(object->physical_path(), "/", tokens);
                            std::string bucket_name = tokens.front();
                            std::string object_name = tokens.back();

                            // reverse object id
                            std::string obj_id = boost::lexical_cast<std::string>(object->id());
                            std::reverse(obj_id.begin(), obj_id.end());

                            // make S3 key name
                            std::ostringstream s3_key_name;
                            s3_key_name << "/" << bucket_name << "/" << obj_id << "/" << object_name;

                            // update physical path
                            object->physical_path(s3_key_name.str());
                        }

                        ret = s3PutCopyFile(S3_PUTFILE, _cache_file_name, object->physical_path(), statbuf.st_size, key_id, access_key, _ctx.prop_map());
                        result = ASSERT_PASS(ret, "Failed to copy the cache file: \"%s\" to the S3 object: \"%s\".",
                                             _cache_file_name, object->physical_path().c_str());

                    }
                }
            }
        }
        if( !result.ok() ) {
            irods::log( result );
        }
        return result;
    } // s3SyncToArchPlugin


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

        // =-=-=-=-=-=-=-
        // check the context validity
        ret = _ctx.valid< irods::file_object >();
        if((result = ASSERT_PASS(ret, "Invalid resource context.")).ok()) {

            // =-=-=-=-=-=-=-
            // check incoming parameters
            if((result = ASSERT_ERROR(_opr && _curr_host && _out_parser && _out_vote, SYS_INVALID_INPUT_PARAM,
                                      "One or more NULL pointer arguments.")).ok()) {

                std::string resc_name;

                // =-=-=-=-=-=-=-
                // cast down the chain to our understood object type
                irods::file_object_ptr file_obj = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );

                // =-=-=-=-=-=-=-
                // get the name of this resource
                ret = _ctx.prop_map().get< std::string >( irods::RESOURCE_NAME, resc_name );
                if((result = ASSERT_PASS(ret, "Failed to get resource name property.")).ok() ) {

                    // =-=-=-=-=-=-=-
                    // add ourselves to the hierarchy parser by default
                    _out_parser->add_child( resc_name );

                    // =-=-=-=-=-=-=-
                    // test the operation to determine which choices to make
                    if( irods::OPEN_OPERATION == (*_opr) ) {
                        // =-=-=-=-=-=-=-
                        // call redirect determination for 'get' operation
                        result = s3RedirectOpen(
                                     _ctx.comm(),
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
                        result = ASSERT_ERROR(false, SYS_INVALID_INPUT_PARAM, "Unknown redirect operation: \"%s\".",
                                              _opr->c_str());
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

}
