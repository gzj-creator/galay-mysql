include_guard(GLOBAL)

# ======================== Build Switches ========================

option(GALAY_MYSQL_BUILD_TESTS "Build galay-mysql tests" ON)
option(GALAY_MYSQL_BUILD_EXAMPLES "Build galay-mysql examples" ON)
option(GALAY_MYSQL_BUILD_SHARED_LIBS "Build galay-mysql as a shared library" ON)
option(GALAY_MYSQL_CXX_EXTENSIONS "Enable compiler-specific C++ extensions" OFF)
option(GALAY_MYSQL_INSTALL_MODULE_INTERFACE "Install C++ module interface files (*.cppm)" ON)
option(GALAY_MYSQL_ENABLE_IMPORT_COMPILATION "Enable C++ import/module compilation path" ON)
option(GALAY_MYSQL_BUILD_MODULE_EXAMPLES "Build examples that use import/module syntax" ON)

if(DEFINED GALAY_MYSQL_BUILD_IMPORT_EXAMPLES)
    message(DEPRECATION "GALAY_MYSQL_BUILD_IMPORT_EXAMPLES is deprecated. "
                        "Use GALAY_MYSQL_BUILD_MODULE_EXAMPLES instead.")
    set(GALAY_MYSQL_BUILD_MODULE_EXAMPLES
        "${GALAY_MYSQL_BUILD_IMPORT_EXAMPLES}"
        CACHE BOOL "Build examples that use import/module syntax" FORCE)
endif()

set(GALAY_MYSQL_CXX_STANDARD "23" CACHE STRING "C++ standard for galay-mysql targets")
set_property(CACHE GALAY_MYSQL_CXX_STANDARD PROPERTY STRINGS 20 23 26)

if(GALAY_MYSQL_BUILD_SHARED_LIBS)
    set(GALAY_MYSQL_LIBRARY_TYPE SHARED)
else()
    set(GALAY_MYSQL_LIBRARY_TYPE STATIC)
endif()

set(GALAY_MYSQL_IMPORT_BUILD_AVAILABLE OFF)
set(GALAY_MYSQL_IMPORT_COMPILE_OPTIONS "")
set(GALAY_MYSQL_IMPORT_UNAVAILABLE_REASON "")
set(GALAY_MYSQL_CLANG_SCAN_DEPS "")

# Note: currently only enable import compilation on a known-stable path.
if(CMAKE_VERSION VERSION_LESS 3.28)
    set(GALAY_MYSQL_IMPORT_UNAVAILABLE_REASON "CMake < 3.28")
else()
    if(NOT CMAKE_GENERATOR MATCHES "Ninja|Visual Studio")
        set(GALAY_MYSQL_IMPORT_UNAVAILABLE_REASON "unsupported generator; require Ninja or Visual Studio")
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
            set(GALAY_MYSQL_IMPORT_UNAVAILABLE_REASON "current stable GNU path is Linux only")
        elseif(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
            set(GALAY_MYSQL_IMPORT_UNAVAILABLE_REASON "GCC < 14")
        else()
            set(GALAY_MYSQL_IMPORT_BUILD_AVAILABLE ON)
        endif()
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        find_program(GALAY_MYSQL_CLANG_SCAN_DEPS NAMES clang-scan-deps)
        if(NOT GALAY_MYSQL_CLANG_SCAN_DEPS)
            set(GALAY_MYSQL_IMPORT_UNAVAILABLE_REASON "clang-scan-deps not found")
        else()
            set(GALAY_MYSQL_IMPORT_UNAVAILABLE_REASON "Clang import path is not enabled yet in this project")
        endif()
    else()
        set(GALAY_MYSQL_IMPORT_UNAVAILABLE_REASON
            "unsupported compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
    endif()
endif()

set(GALAY_MYSQL_IMPORT_COMPILATION_ENABLED OFF)
if(GALAY_MYSQL_ENABLE_IMPORT_COMPILATION AND GALAY_MYSQL_IMPORT_BUILD_AVAILABLE)
    set(GALAY_MYSQL_IMPORT_COMPILATION_ENABLED ON)
elseif(GALAY_MYSQL_ENABLE_IMPORT_COMPILATION AND NOT GALAY_MYSQL_IMPORT_BUILD_AVAILABLE)
    message(STATUS "GALAY_MYSQL_ENABLE_IMPORT_COMPILATION=ON but toolchain is unsupported. "
                   "Import compilation is disabled. "
                   "Requirement: CMake >= 3.28, Ninja/Visual Studio generator, Linux + GCC >= 14. "
                   "Current: ${CMAKE_SYSTEM_NAME}, ${CMAKE_GENERATOR}, ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}. "
                   "Reason: ${GALAY_MYSQL_IMPORT_UNAVAILABLE_REASON}.")
endif()

set(GALAY_MYSQL_BUILD_MODULE_EXAMPLES_EFFECTIVE OFF)
if(GALAY_MYSQL_BUILD_MODULE_EXAMPLES AND GALAY_MYSQL_IMPORT_COMPILATION_ENABLED)
    set(GALAY_MYSQL_BUILD_MODULE_EXAMPLES_EFFECTIVE ON)
elseif(GALAY_MYSQL_BUILD_MODULE_EXAMPLES)
    message(STATUS "GALAY_MYSQL_BUILD_MODULE_EXAMPLES=ON but import compilation is disabled.")
endif()

# Backward compatibility for existing CMake logic/messages.
set(GALAY_MYSQL_BUILD_IMPORT_EXAMPLES_EFFECTIVE ${GALAY_MYSQL_BUILD_MODULE_EXAMPLES_EFFECTIVE})

function(galay_mysql_apply_cxx target_name)
    set_target_properties(${target_name} PROPERTIES
        CXX_STANDARD ${GALAY_MYSQL_CXX_STANDARD}
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS ${GALAY_MYSQL_CXX_EXTENSIONS}
    )
endfunction()
