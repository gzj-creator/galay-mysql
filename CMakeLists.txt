cmake_minimum_required(VERSION 3.20)
project(galay-mysql VERSION 1.2.5 LANGUAGES CXX)

set(CMAKE_FIND_PACKAGE_PREFER_CONFIG ON)
set(CMAKE_NO_SYSTEM_FROM_IMPORTED ON)

set(GALAY_MYSQL_LOCAL_PREFIX_HINTS
    "${CMAKE_SOURCE_DIR}/../.galay-prefix/latest"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/install-local"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/_install-smoke-344"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/_install-smoke"
)

if(NOT DEFINED galay-kernel_DIR OR galay-kernel_DIR STREQUAL "")
    foreach(_galay_mysql_prefix IN LISTS GALAY_MYSQL_LOCAL_PREFIX_HINTS)
        if(EXISTS "${_galay_mysql_prefix}/lib/cmake/galay-kernel/galay-kernel-config.cmake")
            set(galay-kernel_DIR
                "${_galay_mysql_prefix}/lib/cmake/galay-kernel"
                CACHE PATH "Preferred local galay-kernel package directory" FORCE)
            break()
        endif()
    endforeach()
endif()

set(GALAY_MYSQL_LOCAL_DEP_INCLUDE_DIRS "")
foreach(_galay_mysql_local_dep_prefix IN LISTS GALAY_MYSQL_LOCAL_PREFIX_HINTS)
    if(EXISTS "${_galay_mysql_local_dep_prefix}/include")
        list(APPEND GALAY_MYSQL_LOCAL_DEP_INCLUDE_DIRS "${_galay_mysql_local_dep_prefix}/include")
    endif()
endforeach()
list(REMOVE_DUPLICATES GALAY_MYSQL_LOCAL_DEP_INCLUDE_DIRS)

foreach(prefix IN LISTS GALAY_MYSQL_LOCAL_PREFIX_HINTS)
    if(EXISTS "${prefix}")
        list(PREPEND CMAKE_PREFIX_PATH "${prefix}")
    endif()
endforeach()
list(REMOVE_DUPLICATES CMAKE_PREFIX_PATH)

include(cmake/option.cmake)

set(GALAY_MYSQL_BUILD_TESTS_COMPAT FALSE)
if(DEFINED GALAY_MYSQL_BUILD_TESTS AND NOT DEFINED BUILD_TESTING)
    set(BUILD_TESTING ${GALAY_MYSQL_BUILD_TESTS} CACHE BOOL "Build the testing tree." FORCE)
    set(GALAY_MYSQL_BUILD_TESTS_COMPAT TRUE)
elseif(NOT DEFINED BUILD_TESTING)
    set(BUILD_TESTING OFF CACHE BOOL "Build the testing tree." FORCE)
endif()

set(CMAKE_SKIP_TEST_ALL_DEPENDENCY FALSE)
include(CTest)

if(GALAY_MYSQL_BUILD_TESTS_COMPAT)
    message(DEPRECATION "GALAY_MYSQL_BUILD_TESTS is deprecated; use BUILD_TESTING instead.")
endif()

message(STATUS "GALAY_MYSQL_CXX_STANDARD: ${GALAY_MYSQL_CXX_STANDARD}")
message(STATUS "BUILD_TESTING: ${BUILD_TESTING}")
message(STATUS "GALAY_MYSQL_BUILD_TESTS (compat): ${GALAY_MYSQL_BUILD_TESTS}")
message(STATUS "GALAY_MYSQL_BUILD_EXAMPLES: ${GALAY_MYSQL_BUILD_EXAMPLES}")
message(STATUS "GALAY_MYSQL_BUILD_BENCHMARKS: ${GALAY_MYSQL_BUILD_BENCHMARKS}")
message(STATUS "GALAY_MYSQL_BUILD_SHARED_LIBS: ${GALAY_MYSQL_BUILD_SHARED_LIBS}")
message(STATUS "GALAY_MYSQL_INSTALL_MODULE_INTERFACE: ${GALAY_MYSQL_INSTALL_MODULE_INTERFACE}")
message(STATUS "GALAY_MYSQL_IMPORT_COMPILATION_ENABLED: ${GALAY_MYSQL_IMPORT_COMPILATION_ENABLED}")
message(STATUS "GALAY_MYSQL_BUILD_MODULE_EXAMPLES: ${GALAY_MYSQL_BUILD_MODULE_EXAMPLES}")
message(STATUS "GALAY_MYSQL_BUILD_MODULE_EXAMPLES_EFFECTIVE: ${GALAY_MYSQL_BUILD_MODULE_EXAMPLES_EFFECTIVE}")

add_subdirectory(galay-mysql)
if(BUILD_TESTING)
    add_subdirectory(test)
endif()
if(GALAY_MYSQL_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
if(GALAY_MYSQL_BUILD_BENCHMARKS)
    add_subdirectory(benchmark)
endif()
