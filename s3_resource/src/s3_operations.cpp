// =-=-=-=-=-=-=-
// local includes
#include "irods/private/s3_resource/s3_operations.hpp"
#include "irods/private/s3_resource/s3_resource.hpp"
#include "irods/private/s3_transport/s3_transport.hpp"
#include "irods/private/s3_transport/managed_shared_memory_object.hpp"
#include "irods/private/s3_resource/s3_plugin_logging_category.hpp"
#include "irods/private/s3_resource/multipart_shared_data.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include <irods/msParam.h>
#include <irods/rcConnect.h>
#include <irods/rodsErrorTable.h>
#include <irods/objInfo.h>
#include <irods/rsRegReplica.hpp>
#include <irods/dataObjOpr.hpp>
#include <irods/irods_string_tokenize.hpp>
#include <irods/irods_resource_plugin.hpp>
#include <irods/irods_resource_redirect.hpp>
#include <irods/irods_collection_object.hpp>
#include <irods/irods_stacktrace.hpp>
#include <irods/irods_random.hpp>
#include <irods/irods_resource_backport.hpp>
#include <irods/dstream.hpp>
#include <irods/irods_hierarchy_parser.hpp>
#include <irods/irods_virtual_path.hpp>
#include <irods/irods_query.hpp>
#include <irods/voting.hpp>
#include <irods/get_file_descriptor_info.h>
#include <irods/rsModAVUMetadata.hpp>
#include <irods/irods_at_scope_exit.hpp>

// =-=-=-=-=-=-=-
// boost includes
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/interprocess/exceptions.hpp>
#include <boost/interprocess/sync/named_semaphore.hpp>

// =-=-=-=-=-=-=-
// other includes
#include <string>
#include <sstream>
#include <iomanip>
#include <fcntl.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/tree.h>
#include <cstdlib>
#include <list>
#include <map>
#include <assert.h>
#include <curl/curl.h>
#include <fmt/format.h>
#include <filesystem>

extern std::size_t g_retry_count;
extern std::size_t g_retry_wait;

extern thread_local S3ResponseProperties savedProperties;

using odstream            = irods::experimental::io::odstream;
using idstream            = irods::experimental::io::idstream;
using dstream             = irods::experimental::io::dstream;
using s3_transport        = irods::experimental::io::s3_transport::s3_transport<char>;
using s3_transport_config = irods::experimental::io::s3_transport::config;

namespace irods_s3 {

    inline static const std::string SHARED_MEMORY_KEY_PREFIX{"irods_s3-shm-"};
    inline static constexpr int     DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS{180};

    // See https://groups.google.com/g/boost-list/c/5ADnEPYg-ho for an explanation
    // of why the 100*sizeof(void*) is used below.  Essentially, the shared memory
    // must have enough space for the memory algorithm and reserved area but there is
    // no way of knowing the size for these.  It is stated that 100*sizeof(void*) would
    // be enough.
    inline static constexpr std::int64_t SHMEM_SIZE{100*sizeof(void*) + sizeof(multipart_shared_data) }; 
    namespace log  = irods::experimental::log;
    using logger = log::logger<s3_plugin_logging_category>;

    std::mutex global_mutex;
    std::int64_t data_size = s3_transport_config::UNKNOWN_OBJECT_SIZE;
    int number_of_threads = 0;
    int oprType = -1;

    // data per thread
    struct per_thread_data {
        std::ios_base::openmode open_mode;
        std::shared_ptr<dstream> dstream_ptr;
        std::shared_ptr<s3_transport> s3_transport_ptr;
    }; // end per_thread_data

    class fd_to_data_map {

        public:

            fd_to_data_map() : fd_counter{3} {
            }

            fd_to_data_map(const fd_to_data_map& src) = delete;
            fd_to_data_map& operator=(fd_to_data_map& src) = delete;

            per_thread_data get(int fd) {

                std::lock_guard lock(fd_to_data_map_mutex);
                assert(data_map.find(fd) != data_map.end());
                return data_map[fd];
            }

            void set(int fd, const per_thread_data data) {
                std::lock_guard lock(fd_to_data_map_mutex);
                data_map[fd] = data;
            }

            void remove(int fd) {
                std::lock_guard lock(fd_to_data_map_mutex);
                if (data_map.find(fd) == data_map.end()) {
					logger::info("{}:{} ({}) fd is not in table", __FILE__, __LINE__, __FUNCTION__);
                } else {
                    data_map.erase(fd);
                }
            }

            bool exists(int fd) {
                std::lock_guard lock(fd_to_data_map_mutex);
                return data_map.find(fd) != data_map.end();
            }

            int get_and_increment_fd_counter() {
                return fd_counter++;
            }

        private:

            std::mutex fd_to_data_map_mutex;
            std::map<int, per_thread_data> data_map;
            int fd_counter;
    }; // end class fd_to_data_map

    fd_to_data_map fd_data;

    bool operation_requires_that_object_exists(std::ios_base::openmode open_mode, int oprType) {

        using std::ios_base;

        bool put_repl_flag = ( oprType == PUT_OPR || oprType == REPLICATE_DEST || oprType == COPY_DEST );

        const auto m = open_mode & ~(ios_base::ate | ios_base::binary);

        // read only, object must exist
        if (ios_base::in == m) {
            return true;
        }

        // full file upload, object need not exist
        if (put_repl_flag) {
            return false;

        }

        // both input and output, object must exist
        if ((ios_base::out | ios_base::in) == m) {
            return true;
        }

        // default - object need not exist
        return false;
    } // end operation_requires_that_object_exists

    std::string get_shmem_key(irods::plugin_context& ctx, irods::file_object_ptr file_obj) {
        return SHARED_MEMORY_KEY_PREFIX +
            std::to_string(std::hash<std::string>{}(get_resource_name(ctx.prop_map()) + file_obj->logical_path()));
    }

    irods::error s3_file_stat_operation_with_flag_for_retry_on_not_found(irods::plugin_context& _ctx,
            struct stat* _statbuf, bool retry_on_not_found );

    // determines the data size and number of threads, stores them, and returns them
    auto get_number_of_threads_data_size_and_opr_type(irods::plugin_context& _ctx,
                                                      int& number_of_threads,
                                                      int64_t& data_size,
                                                      int& oprType,
                                                      bool query_metadata = true) -> irods::error
    {
        using logger_config = irods::experimental::log::logger_config<s3_plugin_logging_category>;
        using named_shared_memory_object =
            irods::experimental::interprocess::shared_memory::named_shared_memory_object
            <multipart_shared_data>;
        std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
        number_of_threads = 0;

        // Open shared memory and see if we know the number of threads from another thread
        std::string shmem_key = get_shmem_key(_ctx, file_obj);
        logger::trace("{}:{} ({}) [[{}]] shmem_key={} hashed_string={}", __FILE__, __LINE__, __FUNCTION__, thread_id, shmem_key, get_resource_name(_ctx.prop_map()) + file_obj->logical_path() );

        named_shared_memory_object shm_obj{shmem_key,
            DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
            SHMEM_SIZE};

        // wrapping this in an atomic_exec so only one thread/process for a specific data object is executed at a time
        std::string func(__func__);
        auto ret_value = shm_obj.atomic_exec([&number_of_threads, &data_size, &oprType, &_ctx, thread_id, file_obj, func](auto& data) {

            oprType = -1;
            int requested_number_of_threads = 0;

            // ********* DEBUG - print L1desc for all
            if (logger_config::get_level() == log::level::debug ||  logger_config::get_level() == log::level::trace) {

                logger::debug("{}:{} ({}) [[{}]] ------------- L1desc ---------------",
                        __FILE__, __LINE__, func, thread_id);
                for (int i = 0; i < NUM_L1_DESC; ++i) {
                    if (L1desc[i].inuseFlag && L1desc[i].dataObjInp && L1desc[i].dataObjInfo) {
                       int thread_count = L1desc[i].dataObjInp->numThreads;
                       int oprType = L1desc[i].dataObjInp->oprType;
                       std::int64_t data_size = L1desc[i].dataSize;
                       logger::debug("{}:{} ({}) [[{}]] [{}][objPath={}][filePath={}][oprType={}]"
                               "[requested_number_of_threads={}][dataSize={}][dataObjInfo->dataSize={}][srcL1descInx={}]",
                               __FILE__, __LINE__, func, thread_id, i, L1desc[i].dataObjInp->objPath,
                               L1desc[i].dataObjInfo->filePath, oprType, thread_count, data_size,
                               L1desc[i].dataObjInfo->dataSize, L1desc[i].srcL1descInx);
                    }
                }
                logger::debug("{}:{} ({}) [[{}]] ------------------------------------",
                        __FILE__, __LINE__, func, thread_id);
            }
            // ********* END DEBUG - print L1desc for all

            if (data.number_of_threads > 0) {
                number_of_threads = data.number_of_threads;
            }
            logger::debug("{}:{} ({}) [[{}]] number_of_threads in shmem = {}", __FILE__, __LINE__, func, thread_id, data.number_of_threads);

            // get data size stored earlier in s3_resolve_resc_hier_operation
            // brackets reduce scope of lock_guard
            {
                std::lock_guard<std::mutex> lock(global_mutex);
                data_size = irods_s3::data_size;
                oprType = irods_s3::oprType;
            }

            // if data size is still unknown, try to get if from DATA_SIZE_KW
            if (data_size == s3_transport_config::UNKNOWN_OBJECT_SIZE) {
                char *data_size_str = getValByKey(&file_obj->cond_input(), DATA_SIZE_KW);
                logger::debug("{}:{} ({}) [[{}]] data_size_str = {}", __FILE__, __LINE__, func, thread_id, fmt::ptr(data_size_str));
                if (data_size_str) {

                    logger::debug("{}:{} ({}) [[{}]] read DATA_SIZE_KW of {}",
                           __FILE__, __LINE__, func, thread_id, data_size_str);

                    try {
                        data_size = boost::lexical_cast<std::uint64_t>(data_size_str);
                    } catch (boost::bad_lexical_cast const& e) {
                        data_size = s3_transport_config::UNKNOWN_OBJECT_SIZE;
                        logger::warn("{}:{} ({}) [[{}]] DATA_SIZE_KW ({}) could not be parsed as std::size_t",
                                __FILE__, __LINE__, func, thread_id, data_size_str);
                    }
                }
            }

            // first try to get requested number of threads, data size, and oprType from L1desc
            // Note: On a replication from an s3 src within a replication node, there are two entries for the
            //   replica - one for PUT and one for REPL_DEST.  During the initial PUT there is only one
            //   entry.  To see if we are doing the PUT or REPL, look for the last entry on the list.
            bool found = false;
            for (int i = 0; i < NUM_L1_DESC; ++i) {
                if (L1desc[i].inuseFlag) {
                    if (L1desc[i].dataObjInp && L1desc[i].dataObjInfo &&
                            L1desc[i].dataObjInp->objPath == file_obj->logical_path()
                            && L1desc[i].dataObjInfo->filePath == file_obj->physical_path()) {

                        found = true;
                        requested_number_of_threads = L1desc[i].dataObjInp->numThreads;
                        oprType = L1desc[i].dataObjInp->oprType;

                        // if data_size is zero or UNKNOWN, try to get it from L1desc
                        if (data_size == s3_transport_config::UNKNOWN_OBJECT_SIZE) {
                            data_size = L1desc[i].dataSize;
                        }
                    }
                } else if (found) {
                    break;
                }
            }

            // special treatment for replication
            // 1) data_size is only available from the REPLICATE_SRC entry so use that.
            // 2) number_of_threads is available in REPLICATE_DEST entry so use that.
            if (oprType == REPLICATE_DEST) {
                bool found_data_size = false;
                bool found_number_of_threads = (number_of_threads > 0);

                for (int i = 0; i < NUM_L1_DESC; ++i) {
                    const auto& l1d = L1desc[i];
                    const auto* dobj_input = l1d.dataObjInp;
                    const auto* dobj_info = l1d.dataObjInfo;

                    if (!l1d.inuseFlag || !dobj_input || dobj_input->objPath != file_obj->logical_path()) {
                        continue;
                    }

                    // get the data size from source dataObjInfo
                    if (!found_data_size && dobj_info && dobj_input->oprType == REPLICATE_SRC) {
                        data_size = dobj_info->dataSize;
                        logger::debug("{}:{} ({}) [[{}]] repl to s3 destination.  setting data_size to {}",
                                __FILE__, __LINE__, func, thread_id, data_size);

                        found_data_size = true;
                    }

                    // get the number_of_threads from destination dataObjInp
                    if (!found_number_of_threads && dobj_input->oprType == REPLICATE_DEST) {
                        number_of_threads = dobj_input->numThreads;
                        logger::debug("{}:{} ({}) [[{}]] repl to s3 destination.  setting number_of_threads to {}",
                                      __FILE__,
                                      __LINE__,
                                      func,
                                      thread_id,
                                      number_of_threads);

                        found_number_of_threads = true;
                    }

                    // once we have both pieces of information break out of for loop
                    if (found_data_size && found_number_of_threads) {
                        break;
                    }
                }

                if (!found_number_of_threads) {
                    return ERROR(SYS_INTERNAL_ERR,
                                 "Replicating from source to destination but was not able to find the replication "
                                 "destination in L1desc table.");
                }
            }

            // if number_of_threads is still unknown, first try readng from NUM_THREADS_KW
            if (number_of_threads <= 0) {

                // try to get number of threads from NUM_THREADS_KW
                char *num_threads_str = getValByKey(&file_obj->cond_input(), NUM_THREADS_KW);
                logger::debug("{}:{} ({}) [[{}]] num_threads_str = {}", __FILE__, __LINE__, __FUNCTION__, thread_id, fmt::ptr(num_threads_str));

                if (num_threads_str) {
                    logger::debug("{}:{} ({}) [[{}]] num_threads_str = {}",
                        __FILE__, __LINE__, func, thread_id, num_threads_str);
                    try {
                        number_of_threads = boost::lexical_cast<int>(num_threads_str);
                        } catch (const boost::bad_lexical_cast &) {
                            number_of_threads = 0;
                            logger::warn("{}:{} ({}) [[{}]] NUM_THREADS_KW ({}) could not be parsed as int",
                                    __FILE__, __LINE__, func, thread_id, num_threads_str);
                        }
                }

		    	// If number of threads was not successfully set above.
		    	if (number_of_threads == 0) {
		    		const int single_buff_sz = irods::get_advanced_setting<const int>(irods::KW_CFG_MAX_SIZE_FOR_SINGLE_BUFFER) * 1024 * 1024;

		    		if (data_size > single_buff_sz && oprType != REPLICATE_DEST) {
		    			number_of_threads = getNumThreads(_ctx.comm(),
		    			                                  data_size,
		    			                                  requested_number_of_threads,
		    			                                  const_cast<KeyValPair*>(&file_obj->cond_input()),
		    			                                  nullptr, // destination resc hier
		    			                                  nullptr, // source resc hier
		    			                                  0);      // opr type - not used
		    		}
		    	}

		    	// If we still don't know the # of threads, set it to 1 unless the oprType is unknown in
                // which case it will remain <= 0 which will force use of cache.
                if (number_of_threads <= 0 && oprType != -1) {
                    number_of_threads = 1;
                }
            }

            logger::debug("{}:{} ({}) [[{}]] number_of_threads set to {}", __FILE__, __LINE__, func, thread_id, number_of_threads);

            // save the number of threads and data_size
            {
                std::lock_guard<std::mutex> lock(global_mutex);
                irods_s3::data_size = data_size;
                irods_s3::oprType = oprType;
            }

            data.number_of_threads = number_of_threads;

            if (data.threads_remaining_to_close <= 0) {
                data.threads_remaining_to_close = number_of_threads;
            }

            // If this is GET_OPR, we do not need the shared memory. Set the threads_remaining_to_close to 0 so the shmem will be
            // deleted immediately. Note that for GET_OPR we don't necessarily know the number of threads (nor do we need it) and
            // this makes it hard to determine when the shared memory can be deleted.
            if (oprType == GET_OPR) {
                data.threads_remaining_to_close = 0;
            }

            return SUCCESS();
        });

        return ret_value;
    }

    // update and return the physical path in case of decoupled naming
    // returns true if path updated, else false
    void update_physical_path_for_decoupled_naming(irods::plugin_context& _ctx)
    {
        std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
        // retrieve archive naming policy from resource plugin context
        std::string archive_naming_policy = CONSISTENT_NAMING; // default
        irods::error ret = _ctx.prop_map().get<std::string>(ARCHIVE_NAMING_POLICY_KW, archive_naming_policy); // get plugin context property
        if(!ret.ok()) {
            logger::error(fmt::format("[{}] {}", get_resource_name(_ctx.prop_map()), ret.result()));
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

            // get data id from L1desc
            int index = -1;
            for (int i = 0; i < NUM_L1_DESC; ++i) {
               if (L1desc[i].inuseFlag) {
                   if (L1desc[i].dataObjInp && L1desc[i].dataObjInfo &&
                           L1desc[i].dataObjInp->objPath == object->logical_path()
                           && L1desc[i].dataObjInfo->filePath == object->physical_path()) {

                       index = i;
                       break;
                   }
                }
            }

			// On redirect there is not an entry in L1desc[]. The following rules explain the behavior in
			// this instance.
			//
			//   1.  s3_notify_operation() gets called on the server the client is connected to.
			//   2.  In s3_notify_operation(), this method gets called with a L1desc[] entry so that index > 0.
			//       The L1desc[] entry is updated along with the object->physical_path() but only if
			//       openType == CREATE. This part ensures the database gets updated with the proper physical path.
			//   3.  On the redirected server, s3_file_create_operation() gets called which also calls
			//       this method. In that case there is no L1desc[] entry but object->physical_path()
			//       needs to be updated so the file is written to the correct location in S3. Do a
			//       GenQuery to get the object_id and use this to set the object->physical_path().

			if (index > 0) {
				// There is a corresponding L1desc[] entry. Look up the object_id in it. Reverse it
				// for the key.  Write the physical_path to the L1desc[] entry as well as object->physical_path().

				std::string obj_id = boost::lexical_cast<std::string>(L1desc[index].dataObjInfo->dataId);
				std::reverse(obj_id.begin(), obj_id.end());

                // make S3 key name
                const auto s3_key_name = fmt::format("/{}/{}/{}", bucket_name, obj_id, object_name);

				logger::debug("{}:{} ({}) [[{}]] updating physical_path to {}",
				              __FILE__,
				              __LINE__,
				              __func__,
				              thread_id,
				              s3_key_name);
				object->physical_path(s3_key_name);
				strncpy(L1desc[index].dataObjInfo->filePath, s3_key_name.c_str(), MAX_NAME_LEN);
				L1desc[index].dataObjInfo->filePath[MAX_NAME_LEN - 1] = '\0';
			}
			else {
				// There is no L1desc[] entry. Look up the object_id via GenQuery. Reverse it
				// for the key.  Write the physical_path to object->physical_path().

				auto path{std::filesystem::path(object->logical_path())};
				std::string query_string = fmt::format("SELECT DATA_ID WHERE DATA_NAME = '{}' AND COLL_NAME = '{}'",
				                                       path.filename().c_str(),
				                                       path.parent_path().c_str());
				for (const auto& row : irods::query<rsComm_t>{_ctx.comm(), query_string}) {
					std::string object_id = row[0];
					std::reverse(object_id.begin(), object_id.end());
					const auto s3_key_name = fmt::format("/{}/{}/{}", bucket_name, object_id, object_name);
					logger::debug("{}:{} ({}) [[{}]] updating physical_path to {}",
					              __FILE__,
					              __LINE__,
					              __FUNCTION__,
					              thread_id,
					              s3_key_name.c_str());
					object->physical_path(s3_key_name);
					break; // data_id is the same for all replicas so we are done
				}
			}
		}
	}

    std::ios_base::openmode translate_open_mode_posix_to_stream(int oflag, const std::string& call_from) noexcept
    {
        using std::ios_base;

        std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

        logger::debug("{}:{} ({})  [[{}]] call_from={} O_WRONLY={}, O_RDWR={}, O_RDONLY={}, O_TRUNC={}, O_CREAT={}, O_APPEND={}",
                __FILE__, __LINE__, __FUNCTION__, thread_id, call_from.c_str(),
                (oflag & O_ACCMODE) == O_WRONLY, (oflag & O_ACCMODE) == O_RDWR, (oflag & O_ACCMODE) == O_RDONLY,
                (oflag & O_TRUNC) != 0, (oflag & O_CREAT) != 0, (oflag & O_APPEND) != 0);

        ios_base::openmode mode = static_cast<ios_base::openmode>(0);

        if ((oflag & O_ACCMODE) == O_WRONLY || (oflag & O_ACCMODE) == O_RDWR) {
            mode |= ios_base::out;
        }

        if ((oflag & O_ACCMODE) == O_RDONLY || (oflag & O_ACCMODE) == O_RDWR) {
            mode |= ios_base::in;
        }

        if ((oflag & O_TRUNC)) {
            mode |= ios_base::trunc;
        }

        if (oflag & O_APPEND) {
            mode |= ios_base::app;
            mode &= ~ios_base::trunc;  // turn off trunc flag
        }

        logger::debug("{}:{} ({}) [[{}]] translated open mode is [app={}][binary={}][in={}][out={}][trunc={}][ate={}]",
                __FILE__,
                __LINE__,
                __FUNCTION__,
                thread_id, 
                (mode & std::ios::app) != 0,
                (mode & std::ios::binary) != 0,
                (mode & std::ios::in) != 0,
                (mode & std::ios::out) != 0,
                (mode & std::ios::trunc) != 0,
                (mode & std::ios::ate) != 0
                );

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

    bool is_cacheless_mode(irods::plugin_property_map& _prop_map) {

        bool cacheless_mode = false;
        bool attached_mode = true;

        std::tie(cacheless_mode, attached_mode) = get_modes_from_properties(_prop_map);

        return cacheless_mode;

    }


    std::tuple<irods::error, std::shared_ptr<dstream>, std::shared_ptr<s3_transport>> make_dstream(
            irods::plugin_context& _ctx,
            const std::string& call_from)
    {
        // issue #2260
        // For multiprocess file writes, s3_file_notify() is not being called.
        // Try updating the physical path right now.
        update_physical_path_for_decoupled_naming(_ctx);

        std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // get the file descriptor
        int fd = file_obj->file_descriptor();

        irods::error ret;
        std::int64_t data_size = s3_transport_config::UNKNOWN_OBJECT_SIZE;
        int oprType = -1;
        int number_of_threads = 0;
        std::string bucket_name;
        std::string object_key;
        std::string access_key;
        std::string secret_access_key;
        unsigned int circular_buffer_size = S3_DEFAULT_CIRCULAR_BUFFER_SIZE;
        unsigned int circular_buffer_timeout_seconds = S3_DEFAULT_CIRCULAR_BUFFER_TIMEOUT_SECONDS;

        // create entry for fd if it doesn't exist
        if (!fd_data.exists(fd)) {
            per_thread_data data;
            fd_data.set(fd, data);
        }

        // if dstream/transport already created just return
        per_thread_data data = fd_data.get(fd);
        if (data.dstream_ptr && data.s3_transport_ptr) {
            return make_tuple(SUCCESS(), data.dstream_ptr, data.s3_transport_ptr);
        }

        ret = parseS3Path(file_obj->physical_path(), bucket_name, object_key, _ctx.prop_map());
        if(!ret.ok()) {
            return std::make_tuple(PASS(ret), data.dstream_ptr, data.s3_transport_ptr);
        }

        logger::debug("{}:{} ({}) [[{}]] [physical_path={}][bucket_name={}][fd={}]",
                __FILE__, __LINE__, __FUNCTION__, thread_id, file_obj->physical_path().c_str(), bucket_name.c_str(), fd);

        ret = s3GetAuthCredentials(_ctx.prop_map(), access_key, secret_access_key);
        if(!ret.ok()) {
            return std::make_tuple(PASS(ret), data.dstream_ptr, data.s3_transport_ptr);
        }

        ret = get_number_of_threads_data_size_and_opr_type(_ctx, number_of_threads, data_size, oprType);
        if (!ret.ok()) {
            return std::make_tuple(PASS(ret), data.dstream_ptr, data.s3_transport_ptr);
        }

        logger::debug("{}:{} ({}) [[{}]] oprType set to {}", __FILE__, __LINE__, __FUNCTION__, thread_id, oprType);
        logger::debug("{}:{} ({}) [[{}]] data_size set to {}", __FILE__, __LINE__, __FUNCTION__, thread_id, data_size);
        logger::debug("{}:{} ({}) [[{}]] number_of_threads={}", __FILE__, __LINE__, __FUNCTION__, thread_id, number_of_threads);

        // read the size of the circular buffer from configuration
        std::string circular_buffer_size_str;
        ret = _ctx.prop_map().get<std::string>(s3_circular_buffer_size, circular_buffer_size_str);
        if (ret.ok()) {
            try {
                circular_buffer_size = boost::lexical_cast<unsigned int>(circular_buffer_size_str);
            } catch (const boost::bad_lexical_cast &) {}
        }

        // minimum circular buffer size is 2 * minimum_part_size
        if (circular_buffer_size < 2) {
            circular_buffer_size = 2;
        }

        // read the circular buffer timeout from configuration
        std::string circular_buffer_timeout_seconds_str;
        ret = _ctx.prop_map().get<std::string>(s3_circular_buffer_timeout_seconds, circular_buffer_timeout_seconds_str);
        if (ret.ok()) {
            try {
                circular_buffer_timeout_seconds = boost::lexical_cast<unsigned int>(circular_buffer_timeout_seconds_str);
            } catch (const boost::bad_lexical_cast &) {}
        }

        std::string s3_cache_dir_str = get_cache_directory(_ctx.prop_map());

        std::string&& hostname = s3GetHostname(_ctx.prop_map());
        s3_transport_config s3_config;
        s3_config.hostname = hostname;
        s3_config.object_size = data_size;
        s3_config.number_of_cache_transfer_threads = s3GetMPUThreads(_ctx.prop_map());      // number of threads created by s3_transport when writing/reading to/from cache
        s3_config.number_of_client_transfer_threads = number_of_threads;                    // number of threads created by client
        s3_config.bytes_this_thread = data_size == s3_transport_config::UNKNOWN_OBJECT_SIZE // if number of threads is 0, cache is forced and bytes_this_thread is n/a
            || number_of_threads == 0 ? 0 : data_size / number_of_threads;
        s3_config.bucket_name = bucket_name;
        s3_config.access_key = access_key;
        s3_config.secret_access_key = secret_access_key;
        s3_config.shared_memory_timeout_in_seconds = 180;
        s3_config.minimum_part_size = s3GetMPUChunksize(_ctx.prop_map());
        s3_config.circular_buffer_size = circular_buffer_size * s3_config.minimum_part_size;
        s3_config.circular_buffer_timeout_seconds = circular_buffer_timeout_seconds;
        s3_config.s3_protocol_str = get_protocol_as_string(_ctx.prop_map());
        s3_config.s3_uri_request_style = s3_get_uri_request_style(_ctx.prop_map()) == S3UriStyleVirtualHost ? "host" : "path";
        s3_config.region_name = get_region_name(_ctx.prop_map());
        s3_config.put_repl_flag = ( oprType == PUT_OPR || oprType == REPLICATE_DEST || oprType == COPY_DEST );
        s3_config.server_encrypt_flag = s3GetServerEncrypt(_ctx.prop_map());
        s3_config.cache_directory = s3_cache_dir_str;
        s3_config.multipart_enabled = s3GetEnableMultiPartUpload (_ctx.prop_map());
        s3_config.retry_count_limit = get_retry_count(_ctx.prop_map());
        s3_config.retry_wait_seconds = get_retry_wait_time_sec(_ctx.prop_map());
        s3_config.max_retry_wait_seconds = get_max_retry_wait_time_sec(_ctx.prop_map());
        s3_config.resource_name = get_resource_name(_ctx.prop_map());
        s3_config.restoration_days = s3_get_restoration_days(_ctx.prop_map());
        s3_config.restoration_tier = s3_get_restoration_tier(_ctx.prop_map());
        s3_config.max_single_part_upload_size = s3GetMaxUploadSizeMB(_ctx.prop_map()) * 1024 * 1024;
        s3_config.non_data_transfer_timeout_seconds = get_non_data_transfer_timeout_seconds(_ctx.prop_map());
        s3_config.s3_storage_class = s3_get_storage_class_from_configuration(_ctx.prop_map());

        auto sts_date_setting = s3GetSTSDate(_ctx.prop_map());
        s3_config.s3_sts_date_str = sts_date_setting == S3STSAmzOnly ? "amz" : sts_date_setting == S3STSAmzAndDate ? "both" : "date";

        logger::debug("{}:{} ({}) [[{}]] [put_repl_flag={}][object_size={}][multipart_enabled={}][minimum_part_size={}] ",
                __FILE__, __LINE__, __FUNCTION__, thread_id, s3_config.put_repl_flag, s3_config.object_size,
                s3_config.multipart_enabled, s3_config.minimum_part_size);

        // get open mode
        std::ios_base::openmode open_mode = data.open_mode;

        // if data_size is 0, this is not a put or it is a put with a zero length file
        // in this case force cache because the user might do seeks and write out
        // of order
        if (data_size == 0) {
            open_mode |= std::ios_base::in;
            data.open_mode = open_mode;
        }

        data.s3_transport_ptr = std::make_shared<s3_transport>(s3_config);
        data.dstream_ptr = std::make_shared<dstream>(*data.s3_transport_ptr, object_key, open_mode);

        irods::error return_error = SUCCESS();

        if (!data.s3_transport_ptr || !data.dstream_ptr) {
            return_error  = ERROR(S3_FILE_OPEN_ERR,
                    fmt::format("[resource_name={}] null dstream or s3_transport encountered",
                    get_resource_name(_ctx.prop_map())));
        } else {
            fd_data.set(fd, data);
            return_error = data.s3_transport_ptr->get_error();
        }

        return std::make_tuple(return_error, data.dstream_ptr, data.s3_transport_ptr);
    }

    // =-=-=-=-=-=-=-
    // interface for file registration
    irods::error s3_registered_operation( irods::plugin_context& _ctx) {

        if (is_cacheless_mode(_ctx.prop_map())) {
            return SUCCESS();
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }
    }

    // =-=-=-=-=-=-=-
    // interface for file unregistration
    irods::error s3_unregistered_operation( irods::plugin_context& _ctx) {

        if (is_cacheless_mode(_ctx.prop_map())) {
            return SUCCESS();
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }
    }

    // =-=-=-=-=-=-=-
    // interface for file modification
    irods::error s3_modified_operation( irods::plugin_context& _ctx) {

        if (is_cacheless_mode(_ctx.prop_map())) {
            return SUCCESS();
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
       }
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX create

    irods::error s3_file_create_operation( irods::plugin_context& _ctx) {

        if (is_cacheless_mode(_ctx.prop_map())) {

            std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

            irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

            std::ios_base::openmode open_mode;

            // fix open mode
            if (0 == file_obj->flags()) {
                open_mode = translate_open_mode_posix_to_stream(O_CREAT | O_WRONLY | O_TRUNC, __FUNCTION__);
            } else {
                open_mode = translate_open_mode_posix_to_stream(file_obj->flags(), __FUNCTION__);
            }

            // update the physical path
            update_physical_path_for_decoupled_naming(_ctx);

            int fd = fd_data.get_and_increment_fd_counter();
            per_thread_data data;
            data.open_mode = open_mode;
            fd_data.set(fd, data);
            file_obj->file_descriptor(fd);

            logger::debug("{}:{} ({}) [[{}]] physical_path = {}", __FILE__, __LINE__, __FUNCTION__, thread_id, file_obj->physical_path().c_str());

            return SUCCESS();

        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    irods::error s3_file_open_operation( irods::plugin_context& _ctx) {


        if (is_cacheless_mode(_ctx.prop_map())) {

            using std::ios_base;
            using irods::experimental::io::s3_transport::object_s3_status;
            using irods::experimental::io::s3_transport::get_object_s3_status;
            using irods::experimental::io::s3_transport::handle_glacier_status;

            logger::debug("{}:{} ({}) [[{}]]", __FILE__, __LINE__, __FUNCTION__,
                    std::hash<std::thread::id>{}(std::this_thread::get_id()));

            irods::error result = SUCCESS();

            std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

            irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

            // get oprType
            // note on replication there will be two matching entries for repl source, one for put and one for repl src
            // get the highest one
            int oprType = -1;
            bool found = false;
            for (int i = 0; i < NUM_L1_DESC; ++i) {
               if (L1desc[i].inuseFlag) {
                   if (L1desc[i].dataObjInp && L1desc[i].dataObjInfo &&
                           L1desc[i].dataObjInp->objPath == file_obj->logical_path()
                           && L1desc[i].dataObjInfo->filePath == file_obj->physical_path()) {

                       found = true;
                       oprType = L1desc[i].dataObjInp->oprType;
                   }
               } else if (found) {
                   break;
               }
            }

            logger::debug("{}:{} ({}) [[{}]] oprType set to {}",
                    __FILE__, __LINE__, __FUNCTION__, thread_id, oprType);

            ios_base::openmode open_mode;

            // Update open_mode when oprType=PUT_OPR.  There are three scenarios to consider: 
            //
            //   1.  The mode is set to O_WRONLY.  This would not stream because the O_CREAT or
            //       O_TRUNC flag are not set.  Update the open flag to O_WRONLY | O_CREAT | O_TRUNC 
            //       as we know a PUT_OPR is always a full file write or overwrite.
            //
            //   2.  The mode is set to O_RDWR.  This happens when there is a write
            //       which will be followed up by a read for the checksum.  This would not
            //       allow streaming because the file is opened in read and write mode. 
            //       As before, update the open flag to O_WRONLY | O_CREAT | O_TRUNC.
            //
            //   2.  The mode is set to O_RDONLY.  This is the read for checksum that follows the
            //       write.   Leave the oprType alone in this scenario. 
            if (oprType == PUT_OPR && (file_obj->flags() & O_WRONLY || file_obj->flags() & O_RDWR)) {
                open_mode = translate_open_mode_posix_to_stream(O_WRONLY | O_CREAT | O_TRUNC, __FUNCTION__);
            } else {
                open_mode = translate_open_mode_posix_to_stream(file_obj->flags(), __FUNCTION__);
            }

            int fd = fd_data.get_and_increment_fd_counter();
            per_thread_data data;
            data.open_mode = open_mode;
            fd_data.set(fd, data);
            file_obj->file_descriptor(fd);

            bool object_must_exist = operation_requires_that_object_exists(open_mode, oprType);

            if (object_must_exist) {

                S3BucketContext bucket_context = {};

                std::string hostname = s3GetHostname(_ctx.prop_map()).c_str();
                std::string region_name = get_region_name(_ctx.prop_map());

                std::string access_key, secret_access_key;
                result = s3GetAuthCredentials(_ctx.prop_map(), access_key, secret_access_key);
                if(!result.ok()) {
                    return PASS(result);
                }

                std::string bucket_name;
                std::string object_key;
                result = parseS3Path(file_obj->physical_path(), bucket_name, object_key, _ctx.prop_map());
                if(!result.ok()) {
                    return PASS(result);
                }

                bucket_context.hostName         = hostname.c_str();
                bucket_context.bucketName       = bucket_name.c_str();
                bucket_context.authRegion       = region_name.c_str();
                bucket_context.accessKeyId      = access_key.c_str();
                bucket_context.secretAccessKey  = secret_access_key.c_str();
                bucket_context.protocol         = s3GetProto(_ctx.prop_map());
                bucket_context.stsDate          = s3GetSTSDate(_ctx.prop_map());
                bucket_context.uriStyle         = s3_get_uri_request_style(_ctx.prop_map());

                // determine if the object exists
                object_s3_status object_status;
                std::string storage_class;
                std::int64_t object_size = 0;
                result = get_object_s3_status(object_key, bucket_context, object_size, object_status, storage_class);
                if (!result.ok()) {
                    addRErrorMsg( &_ctx.comm()->rError, 0, result.result().c_str());
                    return PASS(result);
                }

                logger::debug("{}:{} ({}) object_status = {} storage_class = {}", __FILE__, __LINE__, __FUNCTION__,
                        object_status == object_s3_status::IN_S3 ? "IN_S3" :
                        object_status == object_s3_status::IN_GLACIER ? "IN_GLACIER" :
                        object_status == object_s3_status::IN_GLACIER_RESTORE_IN_PROGRESS ? "IN_GLACIER_RESTORE_IN_PROGRESS" :
                        "DOES_NOT_EXIST",
                        storage_class);

                unsigned int restoration_days = s3_get_restoration_days(_ctx.prop_map());
                const std::string restoration_tier = s3_get_restoration_tier(_ctx.prop_map());
                result = handle_glacier_status(object_key, bucket_context, restoration_days, restoration_tier, object_status, storage_class);
                if (!result.ok()) {
                    addRErrorMsg( &_ctx.comm()->rError, 0, result.result().c_str());
                    return PASS(result);
                }

            }

            return result;

        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    irods::error s3_file_read_operation( irods::plugin_context& _ctx,
                                   void*               _buf,
                                   const int           _len ) {

        if (is_cacheless_mode(_ctx.prop_map())) {

            logger::debug("{}:{} ({}) [[{}]]", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()));

            irods::error result = SUCCESS();

            std::shared_ptr<dstream> dstream_ptr;
            std::shared_ptr<s3_transport> s3_transport_ptr;

            std::tie(result, dstream_ptr, s3_transport_ptr) = make_dstream(_ctx, __FUNCTION__);

            // If an error has occurred somewhere in the transport,
            // short circuit process and return error.
            if (!result.ok()) {
                addRErrorMsg( &_ctx.comm()->rError, 0, result.result().c_str());
                return PASS(result);
            }

            off_t offset = s3_transport_ptr->get_offset();

            dstream_ptr->read(static_cast<char*>(_buf), _len);

            result = s3_transport_ptr->get_error();
            off_t offset2 = s3_transport_ptr->get_offset();
            off_t diff = offset2 - offset;
            if (result.ok()) {
                result.code(diff);
            }

            return result;
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    irods::error s3_file_write_operation( irods::plugin_context& _ctx,
                                    const void*         _buf,
                                    const int           _len ) {

        if (is_cacheless_mode(_ctx.prop_map())) {

            using named_shared_memory_object =
                irods::experimental::interprocess::shared_memory::named_shared_memory_object
                <multipart_shared_data>;

            std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
            logger::debug("{}:{} ({}) [[{}]]", __FILE__, __LINE__, __FUNCTION__, thread_id);

            irods::error result = SUCCESS();

            // make and read dstream_ptr
            std::shared_ptr<dstream> dstream_ptr;
            std::shared_ptr<s3_transport> s3_transport_ptr;

            std::tie(result, dstream_ptr, s3_transport_ptr) = make_dstream(_ctx, __FUNCTION__);

            if (!result.ok()) {
                addRErrorMsg( &_ctx.comm()->rError, 0, result.result().c_str());
                return PASS(result);
            }

            std::uint64_t data_size = 0;
            int number_of_threads;

            // Open shared memory and get the number_of_threads 
            irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
            std::string shmem_key = get_shmem_key(_ctx, file_obj);
            logger::trace("{}:{} ({}) [[{}]] shmem_key={} hashed_string={}", __FILE__, __LINE__, __FUNCTION__, thread_id, shmem_key, get_resource_name(_ctx.prop_map()) + file_obj->logical_path() );

            named_shared_memory_object shm_obj{shmem_key,
                DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                SHMEM_SIZE};

            shm_obj.atomic_exec([&number_of_threads, thread_id](auto& data) {
                logger::debug("{}:{} ({}) [[{}]] number_of_threads in shared memory - {}",
                    __FILE__, __LINE__, __FUNCTION__, thread_id, data.number_of_threads);
                number_of_threads = data.number_of_threads;
            });

            // get data_size
            {
                std::lock_guard<std::mutex> lock(global_mutex);
                data_size = irods_s3::data_size;
            }
            if (number_of_threads == 0) {
                number_of_threads = 1;
            }

            logger::debug("{}:{} ({}) [[{}]] read number_of_threads of {}", __FILE__, __LINE__, __FUNCTION__, thread_id, number_of_threads);

            // determine the part size based on the offset
            off_t offset = s3_transport_ptr->get_offset();
            std::int64_t bytes_this_thread = data_size / number_of_threads;
            if (static_cast<std::int64_t>(offset) >= bytes_this_thread * (number_of_threads-1)) {
                bytes_this_thread += data_size % number_of_threads;
            }

            s3_transport_ptr->set_bytes_this_thread(bytes_this_thread);

            logger::debug("{}:{} ({}) [[{}]] calling dstream_ptr->write of length {}", __FILE__, __LINE__, __FUNCTION__, thread_id, _len);
            dstream_ptr->write(static_cast<const char*>(_buf), _len);

            // note that the upload is occurring in the background so an error will likely not have occurred yet
            result = s3_transport_ptr->get_error();
            if (result.ok()) {
                result.code(_len);
            }
            return result;

        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }

    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error s3_file_close_operation( irods::plugin_context& _ctx ) {


        if (is_cacheless_mode(_ctx.prop_map())) {

            using named_shared_memory_object =
                irods::experimental::interprocess::shared_memory::named_shared_memory_object
                <multipart_shared_data>;

            std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

            irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
            logger::debug("{}:{} ({}) [[{}]] physical_path = {}", __FILE__, __LINE__, __FUNCTION__, thread_id, file_obj->physical_path().c_str());

            int fd = file_obj->file_descriptor();

            if (fd == 0) {
                return SUCCESS();
            }

            std::shared_ptr<dstream> dstream_ptr;
            std::shared_ptr<s3_transport> s3_transport_ptr;

            if (!fd_data.exists(fd)) {
                return ERROR(UNIX_FILE_CLOSE_ERR,
                        fmt::format("[resource_name={}] {} "
                                "fd_data does not have an entry for fd={}.  "
                                "Was the object closed prior to opening or creating?",
                            get_resource_name(_ctx.prop_map()), __FUNCTION__, fd));
            }

            per_thread_data data = fd_data.get(fd);

            // Need to get the oprType to check if this was a write type of operation.
            // If it was and no dstream_ptr was created, then that means there was never a
            // a call to write presumably because the object is zero bytes.  In that case
            // we need to call s3_file_write_operation() to make sure the object is written
            // to S3.
            int number_of_threads; // not used but needed for the following call
            int64_t data_size;     // not used but needed for the following call
            int oprType;
            irods::error result = get_number_of_threads_data_size_and_opr_type(_ctx, number_of_threads, data_size, oprType);
            if (!result.ok()) {
                return result;
            }
            logger::debug("{}:{} ({}) [[{}]] oprType returned is = {}", __FILE__, __LINE__, __FUNCTION__, thread_id, oprType);

            if (!data.dstream_ptr && oprType != REPLICATE_SRC && oprType != COPY_SRC && oprType != GET_OPR) {
                char buff[1];
			    if (const auto err = s3_file_write_operation(_ctx, buff, 0); !err.ok()) {
			    	return PASS(err);
			    }
			    data = fd_data.get(fd);
            }

            fd_data.remove(fd);

            dstream_ptr = data.dstream_ptr;
            s3_transport_ptr = data.s3_transport_ptr;

            if (dstream_ptr && dstream_ptr->is_open()) {
                dstream_ptr->close();
            }

            if (s3_transport_ptr) {
                result = s3_transport_ptr->get_error();
            }

            // Decrement the threads_remaining_to_close counter in shared memory.
            // Not necessary for GET_OPR as the shared memory is not created in that instance.
            if (irods_s3::oprType != GET_OPR) {

                std::string shmem_key = get_shmem_key(_ctx, file_obj);
                named_shared_memory_object shm_obj{shmem_key,
                    DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                    SHMEM_SIZE};

                auto [open_count, ref_count] = shm_obj.atomic_exec([](auto& data) {
                    // shmem freed when threads_remaining_to_close is zero
                    return std::make_pair(--(data.threads_remaining_to_close), data.ref_count);
                });
                logger::trace("{}:{} ({}) [[{}]] shmem_key={} hashed_string={} open_count={} ref_coun={}", __FILE__, __LINE__, __func__, thread_id, shmem_key, get_resource_name(_ctx.prop_map()) + file_obj->logical_path(), open_count, ref_count);
            }

            //  because s3 might not provide immediate consistency for subsequent stats,
            //  do a stat with a retry if not found
            if (s3_transport_ptr && s3_transport_ptr->is_last_file_to_close() && result.ok()) {

                // Reset global variables.  These cached values for these variables are no longer
                // valid once the last close is performed on the data object.
                //
                // This change specifically addresses issue 2122 where upon opening an object for
                // reading for checksum calculations after a replication, the saved oprType of
                // replication was being read rather than reading the new/correct oprType from the
                // L1desc[] table.
                {
                    std::lock_guard<std::mutex> lock(global_mutex);
                    irods_s3::data_size = s3_transport_config::UNKNOWN_OBJECT_SIZE;
                    irods_s3::oprType = -1;
                }

                struct stat statbuf;

                // do not return an error here as this is meant only as a delay until the stat is available
                // if it is still not avaiable after close() returns it will be detected in a subsequent stat
                s3_file_stat_operation_with_flag_for_retry_on_not_found(_ctx, &statbuf, true);
            }

            dstream_ptr.reset();  // make sure dstream is destructed first

            return result;

        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }
    }


    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    irods::error s3_file_unlink_operation(
        irods::plugin_context& _ctx) {

        logger::debug("{}:{} ({}) [[{}]]", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()));

        // =-=-=-=-=-=-=-
        // check incoming parameters
        irods::error ret = s3CheckParams( _ctx );
        if(!ret.ok()) {
            return PASS(ret);
        }

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
                    return PASSMSG(fmt::format("[resource_name={}] {}", get_resource_name(_ctx.prop_map()), ret.result()), ret);
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


        std::string region_name = get_region_name(_ctx.prop_map());

        S3BucketContext bucketContext = {};
        bucketContext.bucketName = bucket.c_str();
        bucketContext.protocol = s3GetProto(_ctx.prop_map());
        bucketContext.stsDate = s3GetSTSDate(_ctx.prop_map());
        bucketContext.uriStyle = s3_get_uri_request_style(_ctx.prop_map());
        bucketContext.accessKeyId = key_id.c_str();
        bucketContext.secretAccessKey = access_key.c_str();
        bucketContext.authRegion = region_name.c_str();

        callback_data_t data;
        S3ResponseHandler responseHandler = { 0, &responseCompleteCallback };

        data = {};
        std::string&& hostname = s3GetHostname(_ctx.prop_map());
        bucketContext.hostName = hostname.c_str();
        data.pCtx = &bucketContext;
        S3_delete_object(
            &bucketContext,
            key.c_str(), 0,
            get_non_data_transfer_timeout_seconds(_ctx.prop_map()) * 1000,    // timeout (ms)
            &responseHandler,
            &data);

        if(data.status != S3StatusOK && data.status != S3StatusHttpErrorNotFound && data.status != S3StatusErrorNoSuchKey) {

            auto msg = fmt::format("[resource_name={}]  - Error unlinking the S3 object: \"{}\"",
                        get_resource_name(_ctx.prop_map()),
                        file_obj->physical_path());

            if(data.status >= 0) {
                msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)data.status));
            }
            return ERROR(S3_FILE_UNLINK_ERR, msg);
        }

        return SUCCESS();

    } // s3_file_unlink_operation


    // =-=-=-=-=-=-=-
    // interface for POSIX Stat
    irods::error s3_file_stat_operation_with_flag_for_retry_on_not_found(
        irods::plugin_context& _ctx,
        struct stat* _statbuf,
        bool retry_on_not_found )
    {
        std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        logger::debug("{}:{} ({}) [[{}]]", __FILE__, __LINE__, __FUNCTION__, thread_id);

        std::size_t retry_count_limit = get_retry_count(_ctx.prop_map());
        std::size_t retry_wait = get_retry_wait_time_sec(_ctx.prop_map());
        std::size_t max_retry_wait = get_max_retry_wait_time_sec(_ctx.prop_map());

        const auto resource_name = get_resource_name(_ctx.prop_map());

        // =-=-=-=-=-=-=-
        // check incoming parameters
        auto ret = s3CheckParams( _ctx );
        if (!ret.ok()) {
            ret = PASSMSG(fmt::format(
                        "[resource_name={}] Invalid parameters or physical path.",
                        resource_name), ret);

            logger::error(ret.result());

            return ret;
        }

        // =-=-=-=-=-=-=-
        // get ref to fco
        irods::data_object_ptr object = boost::dynamic_pointer_cast<irods::data_object>(_ctx.fco());

        std::memset(_statbuf, 0, sizeof(struct stat));

        boost::filesystem::path p(object->physical_path());

        std::string bucket;
        std::string key;
        std::string key_id;
        std::string access_key;

        ret = parseS3Path(object->physical_path(), bucket, key, _ctx.prop_map());
        if (!ret.ok()) {
            ret = PASSMSG(fmt::format(
                        "[resource_name={}] Failed parsing the S3 bucket and key from the physical path: \"{}\".",
                        resource_name, object->physical_path()), ret);

            logger::error(ret.result());

            return ret;
        }

        ret = s3InitPerOperation( _ctx.prop_map() );
        if (!ret.ok()) {
            ret = PASSMSG(fmt::format(
                        "[resource_name={}] Failed to initialize the S3 system.",
                        resource_name), ret);

            logger::error(ret.result());

            return ret;
        }

        ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
        if (!ret.ok()) {
            ret = PASSMSG(fmt::format(
                        "[resource_name={}] Failed to get the S3 credentials properties.",
                        resource_name), ret);

            logger::error(ret.result());

            return ret;
        }

        std::string region_name = get_region_name(_ctx.prop_map());

        callback_data_t data;
        S3BucketContext bucketContext{};

        bucketContext.bucketName = bucket.c_str();
        bucketContext.protocol = s3GetProto(_ctx.prop_map());
        bucketContext.stsDate = s3GetSTSDate(_ctx.prop_map());
        bucketContext.uriStyle = s3_get_uri_request_style(_ctx.prop_map());
        bucketContext.accessKeyId = key_id.c_str();
        bucketContext.secretAccessKey = access_key.c_str();
        bucketContext.authRegion = region_name.c_str();

        S3ResponseHandler headObjectHandler = { &responsePropertiesCallback, &responseCompleteCallbackIgnoreLoggingNotFound};
        std::size_t retry_cnt = 0;
        do {
            std::string&& hostname = s3GetHostname(_ctx.prop_map());
            bucketContext.hostName = hostname.c_str();
            data.pCtx = &bucketContext;

            S3_head_object(&bucketContext, key.c_str(), 0, 0, &headObjectHandler, &data);

            if ((retry_on_not_found && data.status != S3StatusOK) ||
                    (data.status != S3StatusOK && data.status != S3StatusHttpErrorNotFound)) {


                // On not found just sleep for a second and don't do exponential backoff
                if (data.status == S3StatusHttpErrorNotFound) {
                    s3_sleep( 1 );
                } else {
                    s3_sleep( retry_wait );
                    retry_wait *= 2;
                }
                if (retry_wait > max_retry_wait) {
                    retry_wait = max_retry_wait;
                }
            }
        } while ( data.status != S3StatusOK &&
                ( irods::experimental::io::s3_transport::S3_status_is_retryable(data.status) ||
                  ( retry_on_not_found && data.status == S3StatusHttpErrorNotFound ) ) &&
                ++retry_cnt < retry_count_limit );

        if (data.status == S3StatusOK) {
            _statbuf->st_mode = S_IFREG;
            _statbuf->st_nlink = 1;
            _statbuf->st_uid = getuid ();
            _statbuf->st_gid = getgid ();
            _statbuf->st_atime = _statbuf->st_mtime = _statbuf->st_ctime = savedProperties.lastModified;
            _statbuf->st_size = savedProperties.contentLength;

            return SUCCESS();
        }

        if (data.status == S3StatusHttpErrorNotFound && retry_on_not_found) {
            // This is likely a case where read after write consistency has not been reached.
            // Provide a detailed error message and return
            auto msg = fmt::format("[resource_name={}]  - Error stat'ing the S3 object: \"{}\"",
                    resource_name, object->physical_path());

            if(data.status >= 0) {
                msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)data.status));
            }

            ret = ERROR(S3_FILE_STAT_ERR, msg);

            logger::error(ret.result());

            return ret;
        }

        if (data.status == S3StatusHttpErrorNotFound) {
            // assume this is a collection if the key is not found
            _statbuf->st_mode = S_IFDIR;

            return ret;
        }

        auto msg = fmt::format("[resource_name={}]  - Error stat'ing the S3 object: \"{}\"",
                resource_name, object->physical_path());

        if(data.status >= 0) {
            msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)data.status));
        }

        ret = ERROR(S3_FILE_STAT_ERR, msg);

        logger::error(ret.result());

        return ret;
    } // s3_file_stat_operation_with_flag_for_retry_on_not_found

    irods::error s3_file_stat_operation(
        irods::plugin_context& _ctx,
        struct stat* _statbuf )
    {
        if (!is_cacheless_mode(_ctx.prop_map())) {
            return s3_file_stat_operation_with_flag_for_retry_on_not_found(_ctx, _statbuf, false);
        }

        // cacheless mode
        std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        logger::debug("{}:{} ({}) [[{}]]", __FILE__, __LINE__, __FUNCTION__, thread_id);

        // issue 2153 - Sometimes a stat is called before a close. In the case that we
        // are in cacheless mode but using a local cache file, and that cache file has not yet
        // been flushed, do a stat of that cache file instead of doing a HEAD to S3.
        //
        // We need the fd to get the transport object.  Unfortunately for some reason file_obj->file_descriptor()
        // is not set at this point so we will have to search through the L1desc table for
        // the objPath and get the fd from that.
        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
        int fd = 0;
        for (int i = 0; i < NUM_L1_DESC; ++i) {
            if (L1desc[i].inuseFlag && L1desc[i].dataObjInp && L1desc[i].dataObjInfo) {
                if (L1desc[i].dataObjInp->objPath == file_obj->logical_path()) {
                    fd = i;
                    break;
                }
            }
        }

        if (fd_data.exists(fd)) {
            per_thread_data data = fd_data.get(fd);
            if (data.dstream_ptr && data.s3_transport_ptr && data.s3_transport_ptr->is_cache_file_open()) {

                // do a stat on the cache file, populate stat_buf, and return
                std::string cache_file_physical_path = data.s3_transport_ptr->get_cache_file_path();

                const int status = stat(cache_file_physical_path.c_str(), _statbuf );

                // return an error if necessary
                if (status < 0) {
                    const int err_status = UNIX_FILE_STAT_ERR - errno;
                    return ERROR(err_status, fmt::format(
                                "Stat error for \"{}\", errno = \"{}\", status = {}.",
                                cache_file_physical_path.c_str(), strerror(errno), err_status));
                }

                return CODE(status);
            }
        }

        // there is not an open cache file, do the normal HEAD to S3
        return s3_file_stat_operation_with_flag_for_retry_on_not_found(_ctx, _statbuf, false);
    }

    // =-=-=-=-=-=-=-
    // interface for POSIX Fstat
    irods::error s3FileFstatPlugin(  irods::plugin_context& _ctx,
                                     struct stat*           _statbuf ) {

        if (is_cacheless_mode(_ctx.prop_map())) {
            return SUCCESS();
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }

    } // s3FileFstatPlugin

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    irods::error s3_file_lseek_operation(  irods::plugin_context& _ctx,
                                     const long long        _offset,
                                     const int              _whence ) {

        if (is_cacheless_mode(_ctx.prop_map())) {

            std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
            logger::debug("{}:{} ({}) [[{}]]", __FILE__, __LINE__, __FUNCTION__, thread_id);

            irods::error result = SUCCESS();

            std::shared_ptr<dstream> dstream_ptr;
            std::shared_ptr<s3_transport> s3_transport_ptr;

            std::tie(result, dstream_ptr, s3_transport_ptr) = make_dstream(_ctx, __FUNCTION__);

            // If an error has occurred somewhere in the transport,
            // short circuit process and return error.
            if (!result.ok()) {
                addRErrorMsg( &_ctx.comm()->rError, 0, result.result().c_str());
                return PASS(result);
            }

            logger::debug("{}:{} ({}) [[{}]] offset={}", __FILE__, __LINE__, __FUNCTION__, thread_id, _offset);

            std::ios_base::seekdir seek_directive =
                _whence == SEEK_SET ? std::ios_base::beg : (
                        _whence == SEEK_END ? std::ios_base::end : std::ios_base::cur);

            dstream_ptr->seekg(_offset, seek_directive);

            off_t pos = s3_transport_ptr->get_offset();

            result = s3_transport_ptr->get_error();
            if (result.ok()) {
                result.code(pos);
            }

            logger::debug("{}:{} ({}) [[{}] tellg={}", __FILE__, __LINE__, __FUNCTION__, thread_id, pos);

            return result;

        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }

    } // s3_file_lseek_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error s3_file_mkdir_operation(  irods::plugin_context& _ctx ) {

        if (is_cacheless_mode(_ctx.prop_map())) {
            return SUCCESS();
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }

    } // s3_file_mkdir_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX rmdir
    irods::error s3_rmdir_operation(  irods::plugin_context& _ctx ) {

        if (is_cacheless_mode(_ctx.prop_map())) {
            return SUCCESS();
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }

    } // s3_rmdir_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    irods::error s3_opendir_operation( irods::plugin_context& _ctx ) {

        if (is_cacheless_mode(_ctx.prop_map())) {
            return SUCCESS();
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }

    } // s3_opendir_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    irods::error s3_closedir_operation( irods::plugin_context& _ctx) {

        if (is_cacheless_mode(_ctx.prop_map())) {
            return SUCCESS();
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }

    } // s3_closedir_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error s3_readdir_operation( irods::plugin_context& _ctx,
                                      struct rodsDirent**     _dirent_ptr ) {

        if (is_cacheless_mode(_ctx.prop_map())) {

            logger::debug("{}:{} ({}) [[{}]]", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()));

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

                std::size_t retry_count_limit = get_retry_count(_ctx.prop_map());
                std::size_t retry_wait = get_retry_wait_time_sec(_ctx.prop_map());
                std::size_t max_retry_wait = get_max_retry_wait_time_sec(_ctx.prop_map());

                result = s3InitPerOperation( _ctx.prop_map() );
                if(!result.ok()) {
                    return PASS(result);
                }

                std::string key_id, access_key;
                result = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
                if(!result.ok()) {
                    return PASS(result);
                }

                std::string region_name = get_region_name(_ctx.prop_map());

                S3BucketContext bucketContext = {};

                bucketContext.bucketName = bucket.c_str();
                bucketContext.protocol = s3GetProto(_ctx.prop_map());
                bucketContext.stsDate = s3GetSTSDate(_ctx.prop_map());
                bucketContext.uriStyle = s3_get_uri_request_style(_ctx.prop_map());
                bucketContext.accessKeyId = key_id.c_str();
                bucketContext.secretAccessKey = access_key.c_str();
                bucketContext.authRegion = region_name.c_str();

                std::size_t retry_cnt = 0;
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
                            0,                                                           // timeout
                            &list_bucket_handler,                                        // S3ListBucketHandler
                            &cb_data                                                     // void* callback data
                            );

                    if (data.status != S3StatusOK) {
                        s3_sleep( retry_wait );
                        retry_wait *= 2;
                        if (retry_wait > max_retry_wait) {
                            retry_wait = max_retry_wait;
                        }
                    }

                } while ( (data.status != S3StatusOK) &&
                        irods::experimental::io::s3_transport::S3_status_is_retryable(data.status) &&
                        (++retry_cnt < retry_count_limit ) );

                if (data.status != S3StatusOK) {

                    auto msg = fmt::format("[resource_name={}] - Error in S3 listing:  \"{}\"",
                                get_resource_name(_ctx.prop_map()),
                                search_key.c_str());

                    if(data.status >= 0) {
                        msg += fmt::format(" - \"{}\"", S3_get_status_name((S3Status)data.status));
                    }

                    return ERROR(S3_FILE_STAT_ERR, msg);
                }
            }

            *_dirent_ptr = nullptr;
            if (data.returned_objects.size() > 0) {

                std::string current_key = data.returned_objects.front();
                data.returned_objects.pop_front();
                *_dirent_ptr = ( rodsDirent_t* ) malloc( sizeof( rodsDirent_t ) );
                boost::filesystem::path p(current_key.c_str());
                current_key = p.filename().string();
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

        } else {

            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }

    } // s3_readdir_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX rename
    irods::error s3_file_rename_operation( irods::plugin_context& _ctx,
                                     const char*         _new_file_name )
    {
        logger::debug("{}:{} ({}) [[{}]]", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()));

        irods::error result = SUCCESS();
        std::string access_key;
        std::string secret_access_key;

        const auto resource_name = get_resource_name(_ctx.prop_map());

        // retrieve archive naming policy from resource plugin context
        std::string archive_naming_policy = CONSISTENT_NAMING; // default
        auto ret = _ctx.prop_map().get<std::string>(ARCHIVE_NAMING_POLICY_KW, archive_naming_policy); // get plugin context property
        if (!ret.ok()) {
            logger::error(fmt::format("[{}] {}", resource_name, ret.result()));
        }
        boost::to_lower(archive_naming_policy);

        irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // if archive naming policy is decoupled we're done
        if (archive_naming_policy == DECOUPLED_NAMING) {
            object->file_descriptor(ENOSYS);
            return SUCCESS();
        }

        ret = s3GetAuthCredentials(_ctx.prop_map(), access_key, secret_access_key);
        if (!ret.ok()) {
            // TODO: this is to maintain existing behavior but probably not necessary for error cases
            object->physical_path(_new_file_name);

            return PASSMSG(fmt::format(
                        "[resource_name={}] Failed to get S3 credential properties.",
                        resource_name), ret);
        }

        if (!s3_copyobject_disabled(_ctx.prop_map())) {
            // copy the object to the new location
            ret = s3CopyFile(_ctx, object->physical_path(), _new_file_name, access_key, secret_access_key,
                    s3GetProto(_ctx.prop_map()), s3GetSTSDate(_ctx.prop_map()),
                    s3_get_uri_request_style(_ctx.prop_map()));
            if (!ret.ok()) {
                // TODO: this is to maintain existing behavior but probably not necessary for error cases
                object->physical_path(_new_file_name);

                return PASSMSG(fmt::format(
                            "[resource_name={}] Failed to copy object from: \"{}\" to \"{}\".",
                            resource_name, object->physical_path(), _new_file_name), ret);
            }

            // delete the original object
            ret = s3_file_unlink_operation(_ctx);
            if (!ret.ok()) {
                // TODO: this is to maintain existing behavior but probably not necessary for error cases
                object->physical_path(_new_file_name);

                return PASSMSG(fmt::format(
                            "[resource_name={}] Failed to unlink original S3 object: \"{}\".",
                            resource_name, object->physical_path()), ret);
            }

            object->physical_path(_new_file_name);

            return ret;
        }

        // read the buffer size from rods environment
        rodsLong_t buf_size = irods::get_advanced_setting<const int>(irods::KW_CFG_TRANS_BUFFER_SIZE_FOR_PARA_TRANS) * 1024 * 1024;
        char* buf = ( char* )malloc( buf_size );

        const irods::at_scope_exit free_buf { [buf]() {
            free(buf);
        }};

        struct stat statbuf;
        ret = s3_file_stat_operation_with_flag_for_retry_on_not_found(_ctx, &statbuf, false);
        if (!ret.ok()) {
            // TODO: this is to maintain existing behavior but probably not necessary for error cases
            object->physical_path(_new_file_name);

            return PASSMSG(fmt::format(
                        "[resource_name={}] Failed to stat the source file on rename : \"{}\".",
                        resource_name, object->physical_path()), ret);
        }

        std::string bucket_name;
        std::string src_object_key;
        std::string dest_object_key;
        std::string&& hostname = s3GetHostname(_ctx.prop_map());

        // get source object_key
        ret = parseS3Path(object->physical_path(), bucket_name, src_object_key, _ctx.prop_map());
        if (!ret.ok()) {
            return ret;
        }

        // get destination object_key
        ret = parseS3Path(_new_file_name, bucket_name, dest_object_key, _ctx.prop_map());
        if (!ret.ok()) {
            return ret;
        }

        // read from source and write to destination
        s3_transport_config src_s3_config;
        src_s3_config.hostname = hostname;
        src_s3_config.number_of_cache_transfer_threads = 1;
        src_s3_config.number_of_client_transfer_threads = 1;
        src_s3_config.bucket_name = bucket_name;
        src_s3_config.access_key = access_key;
        src_s3_config.secret_access_key = secret_access_key;
        src_s3_config.shared_memory_timeout_in_seconds = 180;
        src_s3_config.region_name = get_region_name(_ctx.prop_map());
        src_s3_config.s3_protocol_str = s3GetProto(_ctx.prop_map());

        s3_transport src_transport_object{src_s3_config};
        idstream src_dstream_object{src_transport_object, src_object_key};

        // get the source object size
        off_t object_size = src_transport_object.get_existing_object_size();

        s3_transport_config dest_s3_config;
        dest_s3_config.hostname = hostname;
        dest_s3_config.number_of_cache_transfer_threads = 1;
        dest_s3_config.bucket_name = bucket_name;
        dest_s3_config.access_key = access_key;
        dest_s3_config.secret_access_key = secret_access_key;
        dest_s3_config.shared_memory_timeout_in_seconds = 180;
        dest_s3_config.region_name = get_region_name(_ctx.prop_map());
        dest_s3_config.put_repl_flag = false;
        dest_s3_config.object_size = object_size;
        dest_s3_config.minimum_part_size = s3GetMPUChunksize(_ctx.prop_map());
        dest_s3_config.circular_buffer_size = 2 * dest_s3_config.minimum_part_size;
        dest_s3_config.s3_protocol_str = s3GetProto(_ctx.prop_map());

        dest_s3_config.number_of_client_transfer_threads = 1;
        s3_transport dest_transport_object{dest_s3_config};
        odstream dest_dstream_object{dest_transport_object, dest_object_key};

        // copy from src to dest
        for (off_t offset = 0; offset < object_size; offset += buf_size) {
            off_t read_write_size = (offset + buf_size) <= object_size ? buf_size : (object_size - offset);
            src_dstream_object.read(buf, read_write_size);
            dest_dstream_object.write(buf, read_write_size);
        }
        src_dstream_object.close();
        dest_dstream_object.close();

        // delete the original file
        result = s3_file_unlink_operation(_ctx);


        // issue 1855 (irods issue 4326) - resources must now set physical path
        object->physical_path(_new_file_name);

        return result;
    } // s3_file_rename_operation

    // =-=-=-=-=-=-=-
    // interface for POSIX truncate
    irods::error s3FileTruncatePlugin(
        irods::plugin_context& _ctx )
    {
        if (is_cacheless_mode(_ctx.prop_map())) {
            return SUCCESS();
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }
    } // s3FileTruncatePlugin


    // interface to determine free space on a device given a path
    irods::error s3_get_fs_freespace_operation(
        irods::plugin_context& _ctx )
    {
        if (is_cacheless_mode(_ctx.prop_map())) {
            return SUCCESS();
        } else {
            return ERROR(SYS_NOT_SUPPORTED,
                    fmt::format("[resource_name={}] {}",
                        get_resource_name(_ctx.prop_map()), __FUNCTION__));
        }
    } // s3_get_fs_freespace_operation

    // =-=-=-=-=-=-=-
    // s3StageToCache - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from filename to cacheFilename. optionalInfo info
    // is not used.
    irods::error s3_stage_to_cache_operation(irods::plugin_context& _ctx,
                                             const char* _cache_file_name)
    {
        using irods::experimental::io::s3_transport::object_s3_status;
        using irods::experimental::io::s3_transport::get_object_s3_status;
        using irods::experimental::io::s3_transport::handle_glacier_status;

        const auto resource_name = get_resource_name(_ctx.prop_map());

        if (is_cacheless_mode(_ctx.prop_map())) {
            return ERROR(SYS_NOT_SUPPORTED, fmt::format(
                        "[resource_name={}] stage-to-cache is not supported for cacheless mode",
                        resource_name));
        }

        // check incoming parameters
        auto ret = s3CheckParams( _ctx );
        if (!ret.ok()) {
            return PASSMSG(fmt::format(
                        "[resource_name={}] Invalid parameters or physical path.",
                        resource_name), ret);
        }

        std::string access_key;
        std::string secret_access_key;
        irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // stat the object and check/handle glacier status

        S3BucketContext bucket_context = {};

        std::string hostname = s3GetHostname(_ctx.prop_map()).c_str();
        std::string region_name = get_region_name(_ctx.prop_map());

        ret = s3GetAuthCredentials(_ctx.prop_map(), access_key, secret_access_key);
        if(!ret.ok()) {
            return PASS(ret);
        }

        std::string bucket_name;
        std::string object_key;
        ret = parseS3Path(object->physical_path(), bucket_name, object_key, _ctx.prop_map());
        if(!ret.ok()) {
            return PASS(ret);
        }

        bucket_context.hostName         = hostname.c_str();
        bucket_context.bucketName       = bucket_name.c_str();
        bucket_context.authRegion       = region_name.c_str();
        bucket_context.accessKeyId      = access_key.c_str();
        bucket_context.secretAccessKey  = secret_access_key.c_str();
        bucket_context.protocol         = s3GetProto(_ctx.prop_map());
        bucket_context.stsDate          = s3GetSTSDate(_ctx.prop_map());
        bucket_context.uriStyle         = s3_get_uri_request_style(_ctx.prop_map());

        // determine if the object exists

        object_s3_status object_status;
        std::string storage_class;
        std::int64_t object_size = 0;
        ret = get_object_s3_status(object_key, bucket_context, object_size, object_status, storage_class);
        if (!ret.ok()) {
            addRErrorMsg( &_ctx.comm()->rError, 0, ret.result().c_str());
            return PASS(ret);
        }

        logger::debug("{}:{} ({}) object_status = {} storage_class = {}", __FILE__, __LINE__, __FUNCTION__,
                object_status == object_s3_status::IN_S3 ? "IN_S3" :
                object_status == object_s3_status::IN_GLACIER ? "IN_GLACIER" :
                object_status == object_s3_status::IN_GLACIER_RESTORE_IN_PROGRESS ? "IN_GLACIER_RESTORE_IN_PROGRESS" :
                "DOES_NOT_EXIST",
                storage_class);

        unsigned int restoration_days = s3_get_restoration_days(_ctx.prop_map());
        const std::string restoration_tier = s3_get_restoration_tier(_ctx.prop_map());
        ret = handle_glacier_status(object_key, bucket_context, restoration_days, restoration_tier, object_status, storage_class);
        if (!ret.ok()) {
            addRErrorMsg( &_ctx.comm()->rError, 0, ret.result().c_str());
            return PASS(ret);
        }

        if (object->size() > 0 && object->size() != static_cast<rodsLong_t>(object_size)) {
            return ERROR(SYS_COPY_LEN_ERR, fmt::format(
                        "[resource_name={}] Error for file: \"{}\" inp data size: {} does not match stat size: {}.",
                        resource_name, object->physical_path(), object->size(), object_size));
        }

        ret = s3GetFile( _cache_file_name, object->physical_path(), object_size, access_key, secret_access_key, _ctx.prop_map());
        if (!ret.ok()) {
            return PASSMSG(fmt::format(
                        "[resource_name={}] Failed to copy the S3 object: \"{}\" to the cache: \"{}\".",
                        resource_name, object->physical_path(), _cache_file_name), ret);
        }

        return ret;

    } // s3_stage_to_cache_operation

    // =-=-=-=-=-=-=-
    // s3SyncToArch - This routine is for testing the TEST_STAGE_FILE_TYPE.
    // Just copy the file from cacheFilename to filename. optionalInfo info
    // is not used.
    irods::error s3_sync_to_arch_operation(
        irods::plugin_context& _ctx,
        const char* _cache_file_name )
    {
        const auto resource_name = get_resource_name(_ctx.prop_map());

        if (is_cacheless_mode(_ctx.prop_map())) {
            return ERROR(SYS_NOT_SUPPORTED, fmt::format(
                        "[resource_name={}] sync-to-archive is not supported for cacheless mode",
                        resource_name));
        }

        // check incoming parameters
        auto ret = s3CheckParams( _ctx );
        if (!ret.ok()) {
            ret = PASSMSG(fmt::format(
                        "[resource_name={}] Invalid parameters or physical path.",
                        resource_name), ret);

            logger::error(ret.result());

            return ret;
        }

        struct stat statbuf;
        std::string key_id;
        std::string access_key;

        irods::file_object_ptr object = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());
        if (const int ec = stat(_cache_file_name, &statbuf); ec < 0) {
            const int err_status = UNIX_FILE_STAT_ERR - errno;

            ret = ERROR(err_status, fmt::format(
                        "[resource_name={}] Failed to stat cache file: \"{}\".",
                        resource_name, _cache_file_name));

            logger::error(ret.result());

            return ret;
        }

        if (0 == (statbuf.st_mode & S_IFREG)) {
            ret = ERROR(UNIX_FILE_STAT_ERR, fmt::format(
                        "[resource_name={}] Cache file: \"{}\" is not a file.",
                        resource_name, _cache_file_name));

            logger::error(ret.result());

            return ret;
        }

        ret = s3GetAuthCredentials(_ctx.prop_map(), key_id, access_key);
        if (!ret.ok()) {
            ret = PASSMSG(fmt::format(
                        "[resource_name={}] Failed to get S3 credential properties.",
                        resource_name), ret);

            logger::error(ret.result());

            return ret;
        }

        // retrieve archive naming policy from resource plugin context
        std::string archive_naming_policy = CONSISTENT_NAMING; // default
        ret = _ctx.prop_map().get<std::string>(ARCHIVE_NAMING_POLICY_KW, archive_naming_policy); // get plugin context property
        if(!ret.ok()) {
            logger::error(fmt::format("[{}] {}", get_resource_name(_ctx.prop_map()), ret.result()));
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
            const auto s3_key_name = fmt::format("/{}/{}/{}", bucket_name, obj_id, object_name);

            // update physical path
            object->physical_path(s3_key_name);
        }

        ret = s3PutCopyFile(S3_PUTFILE, _cache_file_name, object->physical_path(), statbuf.st_size, key_id, access_key, _ctx.prop_map());
        if (!ret.ok()) {
            ret = PASSMSG(fmt::format(
                        "[resource_name={}] Failed to copy the cache file: \"{}\" to the S3 object: \"{}\".",
                        resource_name, _cache_file_name, object->physical_path()), ret);

            logger::error(ret.result());

            return ret;
        }

        return ret;
    } // s3_sync_to_arch_operation

    // =-=-=-=-=-=-=-
    // used to allow the resource to determine which host
    // should provide the requested operation
    irods::error s3_resolve_resc_hier_operation(
        irods::plugin_context& _ctx,
        const std::string*                  _opr,
        const std::string*                  _curr_host,
        irods::hierarchy_parser*            _out_parser,
        float*                              _out_vote )
    {
        using named_shared_memory_object = irods::experimental::interprocess::shared_memory::named_shared_memory_object
            <multipart_shared_data>;

        logger::debug("{}:{} ({}) [[{}]] _opr={} _curr_host={} shmem_size={}", __FILE__, __LINE__, __FUNCTION__, std::hash<std::thread::id>{}(std::this_thread::get_id()),
                _opr == nullptr ? "nullptr" : _opr->c_str(), _curr_host->c_str(), SHMEM_SIZE);

            for (int i = 0; i < NUM_FILE_DESC; ++i) {
                if (FileDesc[i].inuseFlag) {

                   char* hostname = FileDesc[i].rodsServerHost->hostName->name;
                   int localFlag = FileDesc[i].rodsServerHost->localFlag;

                   logger::debug("{}:{} ({}) FileDesc[{}][hostname={}][localFlag={}][fileName={}][objPath={}][rescHier={}]",
                           __FILE__, __LINE__, __FUNCTION__, i, hostname, localFlag, FileDesc[i].fileName,FileDesc[i].objPath,FileDesc[i].rescHier);
                }
            }

        std::uint64_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());

        irods::file_object_ptr file_obj = boost::dynamic_pointer_cast<irods::file_object>(_ctx.fco());

        // read the data size from DATA_SIZE_KW save it
        std::uint64_t data_size = 0;
        char *data_size_str = getValByKey(&file_obj->cond_input(), DATA_SIZE_KW);
        logger::debug("{}:{} ({}) [[{}]] data_size_str = {}", __FILE__, __LINE__, __FUNCTION__, thread_id, fmt::ptr(data_size_str));
        if (data_size_str) {
            try {
                data_size = boost::lexical_cast<std::uint64_t>(data_size_str);

                // save the data size
                std::lock_guard<std::mutex> lock(global_mutex);
                irods_s3::data_size = data_size;

            } catch (boost::bad_lexical_cast const& e) {
                data_size = s3_transport_config::UNKNOWN_OBJECT_SIZE;
                logger::warn("{}:{} ({}) [[{}]] DATA_SIZE_KW ({}) could not be parsed as std::size_t",
                        __FILE__, __LINE__, __FUNCTION__, thread_id, data_size_str);
            }

        }

        // try to get number of threads from NUM_THREADS_KW
        char *num_threads_str = getValByKey(&file_obj->cond_input(), NUM_THREADS_KW);
        logger::debug("{}:{} ({}) [[{}]] num_threads_str = {}", __FILE__, __LINE__, __FUNCTION__, thread_id, fmt::ptr(num_threads_str));

        if (num_threads_str) {
            logger::debug("{}:{} ({}) [[{}]] num_threads_str = {}",
                __FILE__, __LINE__, __FUNCTION__, thread_id, num_threads_str);
            try {

                int number_of_threads = boost::lexical_cast<int>(num_threads_str);

                // save the number of threads
                std::string shmem_key = get_shmem_key(_ctx, file_obj);
                logger::trace("{}:{} ({}) [[{}]] shmem_key={} hashed_string={}", __FILE__, __LINE__, __FUNCTION__, thread_id, shmem_key, get_resource_name(_ctx.prop_map()) + file_obj->logical_path() );

                named_shared_memory_object shm_obj{shmem_key,
                    DEFAULT_SHARED_MEMORY_TIMEOUT_IN_SECONDS,
                    SHMEM_SIZE};

                shm_obj.atomic_exec([number_of_threads](auto& data) {
                    data.number_of_threads = number_of_threads;
                    data.threads_remaining_to_close = number_of_threads;
                });

            } catch (const boost::bad_lexical_cast &) {
                number_of_threads = 0;
                logger::warn("{}:{} ({}) [[{}]] NUM_THREADS_KW ({}) could not be parsed as int",
                        __FILE__, __LINE__, __FUNCTION__, thread_id, num_threads_str);
            }
        }

        namespace irv = irods::experimental::resource::voting;

        if (irods::error ret = _ctx.valid<irods::file_object>(); !ret.ok()) {
            return PASSMSG("Invalid resource context.", ret);
        }

        if (!_opr || !_curr_host || !_out_parser || !_out_vote) {
            return ERROR(SYS_INVALID_INPUT_PARAM, "Invalid input parameter.");
        }

        if (getValByKey(&file_obj->cond_input(), RECURSIVE_OPR__KW)) {
            logger::debug(
                "{}: {} found in cond_input for file_obj",
                __FUNCTION__, RECURSIVE_OPR__KW);
        }

        logger::debug("{}:{} ({}) [[{}]] get_resource_name={}",
                __FILE__, __LINE__, __FUNCTION__, thread_id, irods::get_resource_name(_ctx).c_str());
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
		if (is_cacheless_mode(_ctx.prop_map())) {
			// Must update the physical_path in the L1desc[] table for decoupled naming.
			// In the case of a redirect, this runs on the original connected server and this
			// is the server that updates the database. In update_physical_path_for_decoupled_naming,
			// the update will only happen if it is a create. Anything else uses whatever was
			// previously in the database.
			update_physical_path_for_decoupled_naming(_ctx);
		}
		return SUCCESS();
	} // s3_notify_operation

}
