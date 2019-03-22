/* For copyright information please refer to files in the COPYRIGHT directory
 */

#ifndef S3_CACHELESS_SHAREDMEMORY_HPP
#define S3_CACHELESS_SHAREDMEMORY_HPP

#include <assert.h>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include "irods_error.hpp"
#include <string>
#include <map>
 
namespace irods_s3_cacheless {
    
    #define SHMMAX 30000000
    #define SHM_BASE_ADDR ((void *)0x80000000)
    std::shared_ptr<boost::interprocess::managed_shared_memory> prepareServerSharedMemory( const std::string& _key ); 
    void detachSharedMemory( const std::string& );
    int removeSharedMemory( const std::string& );
    std::shared_ptr<boost::interprocess::managed_shared_memory> prepareNonServerSharedMemory( const std::string& _key ); 
    irods::error getSharedMemoryName( const std::string&, std::string &shared_memory_name );

    std::shared_ptr<boost::interprocess::managed_shared_memory> get_shared_memory_segment();
}
#endif /* S3_CACHELESS_SHAREDMEMORY_HPP */
