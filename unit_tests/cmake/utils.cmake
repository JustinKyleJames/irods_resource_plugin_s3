# utils.cmake
# ~~~~~~~~~~~
# Defines helper functions and other utilities for testing.

function(unset_irods_test_variables)
    unset(IRODS_TEST_TARGET)
    unset(IRODS_TEST_SOURCE_FILES)
    unset(IRODS_TEST_PROVIDES_MAIN)
    unset(IRODS_TEST_INCLUDE_PATH)
    unset(IRODS_TEST_LINK_LIBRARIES)
    unset(IRODS_TEST_LINK_OBJLIBRARIES)
endfunction()
