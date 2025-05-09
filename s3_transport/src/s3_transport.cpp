#include "irods/private/s3_transport/circular_buffer.hpp"

// iRODS includes
#include <irods/transport/transport.hpp>

// misc includes
#include <nlohmann/json.hpp>
#include "libs3/libs3.h"
#include <fmt/format.h>

// stdlib and misc includes
#include <string>
#include <thread>
#include <vector>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <new>
#include <ctime>
#include <random>

// boost includes
#include <boost/algorithm/string/predicate.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

// local includes
#include "irods/private/s3_transport/multipart_shared_data.hpp"
#include "irods/private/s3_transport/s3_transport.hpp"
#include "irods/private/s3_transport/logging_category.hpp"


namespace irods::experimental::io::s3_transport
{
    const int          S3_DEFAULT_CIRCULAR_BUFFER_SIZE = 4;
    const std::string  S3_RESTORATION_TIER_STANDARD{"Standard"};
    const std::string  S3_RESTORATION_TIER_BULK{"Bulk"};
    const std::string  S3_RESTORATION_TIER_EXPEDITED{"Expedited"};
    const unsigned int S3_DEFAULT_RESTORATION_DAYS = 7;
    const std::string  S3_DEFAULT_RESTORATION_TIER{S3_RESTORATION_TIER_STANDARD};

    const std::string  S3_STORAGE_CLASS_STANDARD{"STANDARD"};
    const std::string  S3_STORAGE_CLASS_GLACIER{"GLACIER"};
    const std::string  S3_STORAGE_CLASS_DEEP_ARCHIVE{"DEEP_ARCHIVE"};
    const std::string  S3_STORAGE_CLASS_GLACIER_IR{"GLACIER_IR"};
    const std::string  S3_DEFAULT_STORAGE_CLASS{S3_STORAGE_CLASS_STANDARD};

    namespace log  = irods::experimental::log;
    using logger = log::logger<s3_transport_logging_category>;

    irods::error get_object_s3_status(const std::string& object_key,
            libs3_types::bucket_context& bucket_context,
            std::int64_t& object_size,
            object_s3_status& object_status,
            std::string& storage_class) {

        data_for_head_callback data(bucket_context);

        S3ResponseHandler head_object_handler = { &s3_head_object_callback::on_response_properties,
            &s3_head_object_callback::on_response_complete };

        S3_head_object(&bucket_context, object_key.c_str(), 0, 0, &head_object_handler, &data);

        if (S3StatusOK != data.status) {
            object_status = object_s3_status::DOES_NOT_EXIST;
            return SUCCESS();
        }

        object_size = data.content_length;

        // Note that GLACIER_IR does not need or accept restoration
        if (boost::iequals(data.x_amz_storage_class, S3_STORAGE_CLASS_GLACIER) ||
                boost::iequals(data.x_amz_storage_class, S3_STORAGE_CLASS_DEEP_ARCHIVE)) {

            storage_class = data.x_amz_storage_class;

            if (data.x_amz_restore.find("ongoing-request=\"false\"") != std::string::npos) {
                // already restored
                object_status = object_s3_status::IN_S3;
            } else if (data.x_amz_restore.find("ongoing-request=\"true\"") != std::string::npos) {
                // being restored
                object_status = object_s3_status::IN_GLACIER_RESTORE_IN_PROGRESS;
            } else {
                object_status = object_s3_status::IN_GLACIER;
            }
        } else {
            object_status = object_s3_status::IN_S3;
        }

        return SUCCESS();
    } // end get_object_s3_status

    irods::error handle_glacier_status(const std::string& object_key,
            libs3_types::bucket_context& bucket_context,
            const unsigned int restoration_days,
            const std::string& restoration_tier,
            object_s3_status object_status,
            const std::string &storage_class) {

        irods::error result = SUCCESS();

        switch (object_status) {

            case object_s3_status::IN_S3:

                break;

            case object_s3_status::DOES_NOT_EXIST:

                logger::error("Object does not exist and open mode requires it to exist.");
                result = ERROR(S3_FILE_OPEN_ERR, "Object does not exist and open mode requires it to exist.");
                break;

            case object_s3_status::IN_GLACIER:

                result =  restore_s3_object(object_key, bucket_context, restoration_days, restoration_tier, storage_class);
                break;

            case object_s3_status::IN_GLACIER_RESTORE_IN_PROGRESS:

                // restoration is already in progress
                result = ERROR(REPLICA_IS_BEING_STAGED, fmt::format("Object is in {} and is currently being restored.  "
                    "Try again later.", storage_class));
                break;

            default:

                // invalid object status - should not happen
                result = ERROR(S3_FILE_OPEN_ERR, "Invalid S3 object status detected.");
                break;

        }

        return result;

    } // end handle_glacier_status

    irods::error restore_s3_object(const std::string& object_key,
            libs3_types::bucket_context& bucket_context,
            const unsigned int restoration_days,
            const std::string& restoration_tier,
            const std::string& storage_class) {

        std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

        // restoration tier only valid for GLACIER
        std::string xml;
        if (storage_class == S3_STORAGE_CLASS_DEEP_ARCHIVE) {
            xml = fmt::format("<RestoreRequest>\n "
                              "  <Days>{}</Days>\n"
                              "</RestoreRequest>\n",
                              restoration_days
                              );
        } else {
            xml = fmt::format("<RestoreRequest>\n "
                              "  <Days>{}</Days>\n"
                              "  <GlacierJobParameters>\n"
                              "    <Tier>{}</Tier>\n"
                              "  </GlacierJobParameters>\n"
                              "</RestoreRequest>\n",
                              restoration_days,
                              restoration_tier);
        }

        irods::experimental::io::s3_transport::upload_manager upload_manager(bucket_context);
        upload_manager.remaining = xml.size();
        upload_manager.xml = const_cast<char*>(xml.c_str());
        upload_manager.offset = 0;

        logger::debug("{}:{} ({}) [[{}]] Multipart:  Restoring object {}", __FILE__, __LINE__, __FUNCTION__, thread_id, object_key.c_str());

        logger::debug("{}:{} ({}) [[{}]] [key={}] Request: {}", __FILE__, __LINE__, __FUNCTION__,
                thread_id, object_key.c_str(), xml.c_str() );

        S3RestoreObjectHandler commit_handler
            = { {restore_object_callback::on_response_properties,
                 restore_object_callback::on_response_completion },
                restore_object_callback::on_response };

        S3_restore_object(&bucket_context, object_key.c_str(),
                &commit_handler, upload_manager.remaining, nullptr, 0, &upload_manager);

        logger::debug("{}:{} ({}) [[{}]] [key={}][manager.status={}]", __FILE__, __LINE__,
                __FUNCTION__, thread_id, object_key.c_str(), S3_get_status_name(upload_manager.status));

        if (upload_manager.status != S3StatusOK) {

            logger::error("{}:{} ({}) [[{}]] S3_restore_object returned error [status={}][object_key={}].",
                    __FILE__, __LINE__, __FUNCTION__, thread_id,
                    S3_get_status_name(upload_manager.status), object_key.c_str());

            return ERROR(REPLICA_STAGING_FAILED, fmt::format("Object is in {}, but scheduling restoration failed.", storage_class));
        }

        return ERROR(REPLICA_IS_BEING_STAGED, fmt::format("Object is in {} and has been queued for restoration.  "
                "Try again later.", storage_class));

    } // end restore_s3_object

    int S3_status_is_retryable(S3Status status) {
        return ::S3_status_is_retryable(status) || libs3_types::status_error_unknown == status;
    }


    void print_bucket_context(const libs3_types::bucket_context& bucket_context)
    {
        logger::debug("BucketContext: [hostName={}] [bucketName={}][protocol={}]"
               "[uriStyle={}][accessKeyId={}][secretAccessKey={}]"
               "[securityToken={}][stsDate={}][region={}]",
               bucket_context.hostName == nullptr ? "" : bucket_context.hostName,
               bucket_context.bucketName == nullptr ? "" : bucket_context.bucketName,
               bucket_context.protocol,
               bucket_context.uriStyle,
               bucket_context.accessKeyId == nullptr ? "" : bucket_context.accessKeyId,
               bucket_context.secretAccessKey == nullptr ? "" : bucket_context.secretAccessKey,
               bucket_context.securityToken == nullptr ? "" : bucket_context.securityToken,
               bucket_context.stsDate,
               bucket_context.authRegion);
    }

    void store_and_log_status( libs3_types::status status,
                               const libs3_types::error_details *error,
                               const std::string& function,
                               const libs3_types::bucket_context& saved_bucket_context,
                               libs3_types::status& pStatus,
                               std::uint64_t thread_id )
    {

        if (thread_id == 0) {
            thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        }

        pStatus = status;
        if(status != libs3_types::status_ok && status != S3StatusHttpErrorNotFound) {

            logger::error( "{}:{} [{}] [[{}]]  libs3_types::status: [{}] - {}",
                    __FILE__, __LINE__, __FUNCTION__, thread_id, S3_get_status_name( status ), static_cast<int>(status) );
            if (saved_bucket_context.hostName) {
                logger::error( "{}:{} [{}] [[{}]]  S3Host: {}",
                        __FILE__, __LINE__, __FUNCTION__, thread_id, saved_bucket_context.hostName );
            }

            logger::error( "{}:{} [{}] [[{}]]  Function: {}",
                    __FILE__, __LINE__, __FUNCTION__, thread_id, function.c_str() );

            if (error) {

                if (error->message) {
                    logger::error( "{}:{} [{}] [[{}]]  Message: {}",
                            __FILE__, __LINE__, __FUNCTION__, thread_id, error->message);
                }
                if (error->resource) {
                    logger::error( "{}:{} [{}] [[{}]]  Resource: {}",
                            __FILE__, __LINE__, __FUNCTION__, thread_id, error->resource);
                }
                if (error->furtherDetails) {
                    logger::error( "{}:{} [{}] [[{}]]  Further Details: {}",
                            __FILE__, __LINE__, __FUNCTION__, thread_id, error->furtherDetails);
                }
                if (error->extraDetailsCount) {
                    logger::error( "{}:{} [{}] [[{}]]{}",
                            __FILE__, __LINE__, __FUNCTION__, thread_id, "  Extra Details:");

                    for (int i = 0; i < error->extraDetailsCount; i++) {
                        logger::error( "{}:{} [{}] [[{}]]    {}: {}",
                                __FILE__, __LINE__, __FUNCTION__, thread_id, error->extraDetails[i].name,
                                error->extraDetails[i].value);
                    }
                }
            }

        } else {

            logger::debug( "{}:{} [{}] [[{}]]  libs3_types::status: [{}] - {}",
                    __FILE__, __LINE__, __FUNCTION__, thread_id, S3_get_status_name( status ), static_cast<int>(status) );
            if (saved_bucket_context.hostName) {
                logger::debug( "{}:{} [{}] [[{}]]  S3Host: {}",
                        __FILE__, __LINE__, __FUNCTION__, thread_id, saved_bucket_context.hostName );
            }

            logger::debug( "{}:{} [{}] [[{}]]  Function: {}",
                    __FILE__, __LINE__, __FUNCTION__, thread_id, function.c_str() );

            if (error) {

                if (error->message) {
                    logger::debug( "{}:{} [{}] [[{}]]  Message: {}",
                            __FILE__, __LINE__, __FUNCTION__, thread_id, error->message);
                }
                if (error->resource) {
                    logger::debug( "{}:{} [{}] [[{}]]  Resource: {}",
                            __FILE__, __LINE__, __FUNCTION__, thread_id, error->resource);
                }
                if (error->furtherDetails) {
                    logger::debug( "{}:{} [{}] [[{}]]  Further Details: {}",
                            __FILE__, __LINE__, __FUNCTION__, thread_id, error->furtherDetails);
                }
                if (error->extraDetailsCount) {
                    logger::debug( "{}:{} [{}] [[{}]]{}",
                            __FILE__, __LINE__, __FUNCTION__, thread_id, "  Extra Details:");

                    for (int i = 0; i < error->extraDetailsCount; i++) {
                        logger::debug( "{}:{} [{}] [[{}]]    {}: {}",
                                __FILE__, __LINE__, __FUNCTION__, thread_id, error->extraDetails[i].name,
                                error->extraDetails[i].value);
                    }
                }
            }
       }
    }  // end store_and_log_status

    // Returns timestamp in usec for delta-t comparisons
    // std::uint64_t provides plenty of headroom
    std::uint64_t get_time_in_microseconds()
    {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return (tv.tv_sec) * 1000000LL + tv.tv_usec;
    } // end get_time_in_microseconds

    // Sleep between _s/2 to _s.
    // The random addition ensures that threads don't all cluster up and retry
    // at the same time (dogpile effect)
    void s3_sleep(
        int _s) {

        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int> uniform_dist(0, RAND_MAX);
        int random = uniform_dist(e1);
        int sleep_time = (int)((((double)random / (double)RAND_MAX) + 1) * .5 * _s); // sleep between _s/2 and _s
        std::this_thread::sleep_for (std::chrono::seconds (sleep_time));
    }

    namespace s3_head_object_callback
    {
        libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                    void *callback_data)
        {

            data_for_head_callback *data = (data_for_head_callback*)callback_data;
            data->content_length = properties->contentLength;

            // read the headers used by GLACIER
            if (properties->xAmzStorageClass) {
                data->x_amz_storage_class = properties->xAmzStorageClass;
            }
            if (properties->xAmzRestore) {
               data->x_amz_restore = properties->xAmzRestore;
            }

            return libs3_types::status_ok;
        }

        void on_response_complete (libs3_types::status status,
                                   const libs3_types::error_details *error,
                                   void *callback_data)
        {
            data_for_head_callback *data = (data_for_head_callback*)callback_data;
            store_and_log_status( status, error, "s3_head_object_callback::on_response_complete", data->bucket_context,
                    data->status );
        }


    }

    namespace s3_upload
    {

        namespace initialization_callback
        {

            libs3_types::status on_response (const libs3_types::char_type* upload_id,
                                          void *callback_data )
            {
                using named_shared_memory_object =
                    irods::experimental::interprocess::shared_memory::named_shared_memory_object
                    <shared_data::multipart_shared_data>;
                // upload upload_id in shared memory
                // no need to shared_memory_lock as this should already be locked

                // upload upload_id in shared memory
                upload_manager *manager = (upload_manager *)callback_data;

                std::string& shmem_key = manager->shmem_key;

                // upload upload_id in shared memory
                named_shared_memory_object shm_obj{shmem_key,
                    manager->shared_memory_timeout_in_seconds,
                    constants::MAX_S3_SHMEM_SIZE};

                // upload upload_id in shared memory - already locked here
                shm_obj.exec([upload_id](auto& data) {
                    data.upload_id = upload_id;
                });

                // upload upload_id in shared memory
                return libs3_types::status_ok;
            } // end on_response

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                     void *callback_data)
            {
                return libs3_types::status_ok;
            } // end on_response_properties

            void on_response_complete (libs3_types::status status,
                                    const libs3_types::error_details *error,
                                    void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, "s3_upload::on_response_complete", data->saved_bucket_context,
                        data->status );
            } // end on_response_complete

        } // end namespace initialization_callback

        // Uploading the multipart completion XML from our buffer
        namespace commit_callback
        {
            int on_response (int buffer_size,
                          libs3_types::buffer_type buffer,
                          void *callback_data)
            {
                upload_manager *manager = (upload_manager *)callback_data;
                std::int64_t ret = 0;
                if (manager->remaining) {
                    int to_read_count = ((manager->remaining > static_cast<std::int64_t>(buffer_size)) ?
                                  static_cast<std::int64_t>(buffer_size) : manager->remaining);
                    memcpy(buffer, manager->xml.c_str() + manager->offset, to_read_count);
                    ret = to_read_count;
                }
                manager->remaining -= ret;
                manager->offset += ret;

                return static_cast<int>(ret);
            } // end commit

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return libs3_types::status_ok;
            } // end response_properties

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, "s3_upload::on_response_completion", data->saved_bucket_context,
                        data->status );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion


        } // end namespace commit_callback



        namespace cancel_callback
        {
            libs3_types::status g_response_completion_status = libs3_types::status_ok;
            libs3_types::bucket_context *g_response_completion_saved_bucket_context = nullptr;

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return libs3_types::status_ok;
            } // response_properties

            // S3_abort_multipart_upload() does not allow a callback_data parameter, so pass the
            // final operation status using this global.

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                store_and_log_status( status, error, "cancel_callback::on_response_completion", *g_response_completion_saved_bucket_context,
                        g_response_completion_status);
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion

        } // end namespace cancel_callback



    } // end namespace s3_upload

    namespace s3_multipart_upload
    {

        namespace initialization_callback
        {

            libs3_types::status on_response (const libs3_types::char_type* upload_id,
                                          void *callback_data )
            {
                using named_shared_memory_object =
                    irods::experimental::interprocess::shared_memory::named_shared_memory_object
                    <shared_data::multipart_shared_data>;
                // upload upload_id in shared memory
                // no need to shared_memory_lock as this should already be locked

                // upload upload_id in shared memory
                upload_manager *manager = (upload_manager *)callback_data;

                std::string& shmem_key = manager->shmem_key;

                named_shared_memory_object shm_obj{shmem_key,
                    manager->shared_memory_timeout_in_seconds,
                    constants::MAX_S3_SHMEM_SIZE};

                // upload upload_id in shared memory - already locked here
                shm_obj.exec([upload_id](auto& data) {
                    data.upload_id = upload_id;
                });

                // upload upload_id in shared memory
                return libs3_types::status_ok;
            } // end on_response

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                                     void *callback_data)
            {
                return libs3_types::status_ok;
            } // end on_response_properties

            void on_response_complete (libs3_types::status status,
                                    const libs3_types::error_details *error,
                                    void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, "s3_multipart_upload::on_response_complete", data->saved_bucket_context,
                        data->status);
            } // end on_response_complete

        } // end namespace initialization_callback

        // Uploading the multipart completion XML from our buffer
        namespace commit_callback
        {
            int on_response (int buffer_size,
                          libs3_types::buffer_type buffer,
                          void *callback_data)
            {
                upload_manager *manager = (upload_manager *)callback_data;
                std::int64_t ret = 0;
                if (manager->remaining) {
                    int to_read_count = ((manager->remaining > static_cast<std::int64_t>(buffer_size)) ?
                                  static_cast<std::int64_t>(buffer_size) : manager->remaining);
                    memcpy(buffer, manager->xml.c_str() + manager->offset, to_read_count);
                    ret = to_read_count;
                }
                manager->remaining -= ret;
                manager->offset += ret;

                return static_cast<int>(ret);
            } // end commit

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return libs3_types::status_ok;
            } // end response_properties

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                upload_manager *data = (upload_manager*)callback_data;
                store_and_log_status( status, error, "s3_multipart_upload::on_response_completion", data->saved_bucket_context,
                        data->status );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion


        } // end namespace commit_callback


        namespace cancel_callback
        {
            libs3_types::status g_response_completion_status = libs3_types::status_ok;
            libs3_types::bucket_context *g_response_completion_saved_bucket_context = nullptr;

            libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                          void *callback_data)
            {
                return libs3_types::status_ok;
            } // response_properties

            // S3_abort_multipart_upload() does not allow a callback_data parameter, so pass the
            // final operation status using this global.

            void on_response_completion (libs3_types::status status,
                                      const libs3_types::error_details *error,
                                      void *callback_data)
            {
                store_and_log_status( status, error, "cancel_callback::on_response_completion", *g_response_completion_saved_bucket_context,
                        g_response_completion_status );
                // Don't change the global error, we may want to retry at a higher level.
                // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
            } // end response_completion

        } // end namespace cancel_callback



    } // end namespace s3_multipart_upload

    namespace restore_object_callback
    {
        int on_response (int buffer_size,
                      libs3_types::buffer_type buffer,
                      void *callback_data)
        {
            upload_manager *manager = (upload_manager *)callback_data;
            int ret = 0;
            if (manager->remaining) {
                int to_read_count = (static_cast<int>(manager->remaining) > buffer_size) ?
                              buffer_size : manager->remaining;
                memcpy(buffer, manager->xml.c_str() + manager->offset, to_read_count);
                ret = to_read_count;
            }
            manager->remaining -= ret;
            manager->offset += ret;

            return ret;
        } // end on_response

        libs3_types::status on_response_properties (const libs3_types::response_properties *properties,
                                      void *callback_data)
        {
            return libs3_types::status_ok;
        } // end on_response_properties

        void on_response_completion (libs3_types::status status,
                                  const libs3_types::error_details *error,
                                  void *callback_data)
        {
            upload_manager *data = (upload_manager*)callback_data;
            if (data) {
                store_and_log_status( status, error, "s3_upload::on_response_completion", data->saved_bucket_context,
                        data->status );
            }
            // Don't change the global error, we may want to retry at a higher level.
            // The WorkerThread will note that status!=OK and act appropriately (retry or fail)
        } // end on_response_completion

    } // end namespace restore_object_callback

}

