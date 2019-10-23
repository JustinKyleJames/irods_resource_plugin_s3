/* For copyright information please refer to files in the COPYRIGHT directory
 */

#include <fcntl.h>
#include "s3_cacheless_sharedmemory.hpp"
#include "rodsConnect.h"
#include "irods_server_properties.hpp"
#include <boost/interprocess/managed_shared_memory.hpp>
#include "irods_stacktrace.hpp"
#include <map>

namespace bi = boost::interprocess;

namespace irods_s3_cacheless {

    // a map which maps the resource name to the shared memory segment for that resource
    std::map<std::string, std::shared_ptr<bi::managed_shared_memory> > shared_memory_segments;

    std::shared_ptr<bi::managed_shared_memory> get_shared_memory_segment(const std::string& s3_resource_name) {

        auto iter = shared_memory_segments.find(s3_resource_name);
        if (iter != shared_memory_segments.end()) {
            return iter->second;
        } else {
            auto tmp = prepareServerSharedMemory(s3_resource_name);
            shared_memory_segments[s3_resource_name] = tmp;
            return tmp;
        }
    }

    std::shared_ptr<bi::managed_shared_memory> prepareServerSharedMemory( const std::string& _key ) {
       
        std::string shared_memory_name;
        irods::error ret = getSharedMemoryName( _key, shared_memory_name );
        if ( !ret.ok() ) {
            rodsLog( LOG_ERROR, "prepareServerSharedMemory: failed to get shared memory name" );
            return nullptr;
        }
    
        try {
            std::shared_ptr<bi::managed_shared_memory> segment = std::make_shared<bi::managed_shared_memory>(bi::open_or_create, shared_memory_name.c_str(), SHMMAX);
            return segment;
        }
        catch ( const bi::interprocess_exception &e ) {
            rodsLog( LOG_ERROR, "prepareServerSharedMemory: failed to prepare shared memory. Exception caught [%s]", e.what() );
            return nullptr;
        }
    }
    
    void detachSharedMemory( const std::string& _key ) {
    }
    
    int removeSharedMemory( const std::string& _key ) {
        std::string shared_memory_name;
        irods::error ret = getSharedMemoryName( _key, shared_memory_name );
        if ( !ret.ok() ) {
            rodsLog( LOG_ERROR, "removeSharedMemory: failed to get shared memory name" );
            return RE_SHM_UNLINK_ERROR;
        }
    
        if ( !bi::shared_memory_object::remove( shared_memory_name.c_str() ) ) {
            rodsLog( LOG_ERROR, "removeSharedMemory: failed to remove shared memory" );
            return RE_SHM_UNLINK_ERROR;
        }
        return 0;
    }
    
/*    std::shared_ptr<bi::managed_shared_memory> prepareNonServerSharedMemory( const std::string& _key ) {

        std::string shared_memory_name;
        irods::error ret = getSharedMemoryName( _key, shared_memory_name );
        if ( !ret.ok() ) {
            rodsLog( LOG_ERROR, "prepareServerSharedMemory: failed to get shared memory name" );
            return nullptr;
        }
    
        try {
            std::shared_ptr<bi::managed_shared_memory> segment = std::make_shared<bi::managed_shared_memory>(bi::open_only, shared_memory_name.c_str(), SHMMAX);
            return segment;
        }
        catch ( const bi::interprocess_exception &e ) {
            rodsLog( LOG_ERROR, "prepareServerSharedMemory: failed to prepare shared memory. Exception caught [%s]", e.what() );
            return nullptr;
        }
    }*/
 
    irods::error getSharedMemoryName( const std::string& _key, std::string &shared_memory_name ) {
        try {
            const auto& shared_memory_name_salt = irods::get_server_property<const std::string>(irods::CFG_RE_CACHE_SALT_KW);
            shared_memory_name = "irods_s3_cacheless_plugin_shared_memory_" + _key + "_" + shared_memory_name_salt;
        } catch ( const irods::exception& e ) {
            rodsLog( LOG_ERROR, "getSharedMemoryName: failed to retrieve re cache salt from server_properties\n%s", e.what() );
            return irods::error(e);
        }
        return SUCCESS();
    }
}
