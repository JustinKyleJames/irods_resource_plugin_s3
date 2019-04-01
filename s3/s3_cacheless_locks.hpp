/* For copyright information please refer to files in the COPYRIGHT directory
 */

#ifndef S3_CACHELESS_LOCKS_HPP 
#define S3_CACHELESS_LOCKS_HPP

#include <boost/interprocess/sync/named_sharable_mutex.hpp>
#include <boost/interprocess/sync/named_upgradable_mutex.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/exceptions.hpp>
#include "irods_error.hpp"
#include <map>

namespace irods_s3_cacheless {

    // Class AutoLockNamedMutex - RAAI wrapper for named_upgradable_mutex
    class AutoLockNamedMutex
    {
     private:
         std::shared_ptr<boost::interprocess::named_upgradable_mutex> auto_mutex;
     public:
         explicit AutoLockNamedMutex(std::shared_ptr<boost::interprocess::named_upgradable_mutex> mtx);
         ~AutoLockNamedMutex();
    };

    typedef boost::interprocess::named_sharable_mutex mutex_type;
    
    void unlockReadMutex( const char*, mutex_type **mutex );
    int lockReadMutex(  const char*, mutex_type **mutex );
    void unlockWriteMutex( const char*, mutex_type **mutex );
    int lockWriteMutex(  const char*, mutex_type **mutex );
    void resetMutex( const char* );
    irods::error getMutexName( const char*, std::string &mutex_name );

    std::shared_ptr<boost::interprocess::named_upgradable_mutex> get_named_mutex(); 
}

#endif
