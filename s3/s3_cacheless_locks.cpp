/* For copyright information please refer to files in the COPYRIGHT directory
 */
#include "s3_cacheless_locks.hpp"
#include "rodsConnect.h"
#include "irods_log.hpp"
#include "irods_server_properties.hpp"
#include <map>

namespace bi = boost::interprocess;

namespace irods_s3_cacheless {

    // a map which maps the resource name to the shared memory mutex for that resource
    std::map<std::string, std::shared_ptr<bi::named_upgradable_mutex> > named_mutexes;

    std::shared_ptr<bi::named_upgradable_mutex> get_named_mutex(const std::string& s3_resource_name) {

        auto iter = named_mutexes.find(s3_resource_name);
        if (iter != named_mutexes.end()) {
            return iter->second;
        } else {
            std::string mutex_name;
            getMutexName( s3_resource_name.c_str(), mutex_name );
            std::shared_ptr<bi::named_upgradable_mutex> tmp = std::make_shared<bi::named_upgradable_mutex>(bi::open_or_create, mutex_name.c_str()); 
            named_mutexes[s3_resource_name] = tmp;
            return tmp;
        }
    }


    int lockWriteMutex( const char* _inst_name, mutex_type **mutex ) {
        std::string mutex_name;
        irods::error ret = getMutexName( _inst_name, mutex_name );
        if ( !ret.ok() ) {
            rodsLog( LOG_ERROR, "lockMutex: call to getMutexName failed" );
            return -1;
        }
    
        try {
            *mutex = new mutex_type( bi::open_or_create, mutex_name.c_str() );
        }
        catch ( const bi::interprocess_exception& ) {
            rodsLog( LOG_ERROR, "bi::named_mutex threw a bi::interprocess_exception." );
            return -1;
        }
        try {
            ( *mutex )->lock();
        }
        catch ( const bi::interprocess_exception& ) {
            rodsLog( LOG_ERROR, "lock threw a bi::interprocess_exception." );
            return -1;
        }
        return 0;
    }
    
    int lockReadMutex( const char* _inst_name, mutex_type **mutex ) {
        std::string mutex_name;
        irods::error ret = getMutexName( _inst_name, mutex_name );
        if ( !ret.ok() ) {
            rodsLog( LOG_ERROR, "lockMutex: call to getMutexName failed" );
            return -1;
        }
    
        try {
            *mutex = new mutex_type( bi::open_or_create, mutex_name.c_str() );
        }
        catch ( const bi::interprocess_exception& ) {
            rodsLog( LOG_ERROR, "bi::named_mutex threw a bi::interprocess_exception." );
            return -1;
        }
        try {
            ( *mutex )->lock_sharable();
        }
        catch ( const bi::interprocess_exception& ) {
            rodsLog( LOG_ERROR, "lock threw a bi::interprocess_exception." );
            return -1;
        }
        return 0;
    }
    
    void unlockWriteMutex( const char* _inst_name, mutex_type **mutex ) {
        try {
            ( *mutex )->unlock();
        }
        catch ( const bi::interprocess_exception& ) {
            rodsLog( LOG_ERROR, "unlock threw a bi::interprocess_exception." );
        }
        delete *mutex;
    }
    
    void unlockReadMutex( const char* _inst_name, mutex_type **mutex ) {
        try {
            ( *mutex )->unlock_sharable();
        }
        catch ( const bi::interprocess_exception& ) {
            rodsLog( LOG_ERROR, "unlock threw a bi::interprocess_exception." );
        }
        delete *mutex;
    }
    
    /* This function can be used during initialization to remove a previously held mutex that has not been released.
     * This should only be used when there is no other process using the mutex */
    void resetMutex(const char* _inst_name) {
        std::string mutex_name;
        irods::error ret = getMutexName( _inst_name, mutex_name );
        if ( !ret.ok() ) {
            rodsLog( LOG_ERROR, "resetMutex: call to getMutexName failed" );
        }
    
        mutex_type::remove( mutex_name.c_str() );
    }
    
    irods::error getMutexName( const char* _inst_name, std::string &mutex_name ) {
        try {
            const auto& mutex_name_salt = irods::get_server_property<const std::string>(irods::CFG_RE_CACHE_SALT_KW);
            mutex_name = "irods_s3_cacheless_plugin_mutex_";
            mutex_name += _inst_name;
            mutex_name += "_";
            mutex_name += mutex_name_salt;
        } catch ( const irods::exception& e ) {
            rodsLog( LOG_ERROR, "getMutexName: failed to retrieve re cache salt from server_properties\n%s", e.what() );
            return irods::error(e);
        }
        return SUCCESS();
    }
}
