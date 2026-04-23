find_package(galay-kernel 3.4.4 CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(spdlog REQUIRED)
find_package(Threads REQUIRED)

if(TARGET spdlog::spdlog_header_only)
    set(GALAY_MYSQL_SPDLOG_TARGET spdlog::spdlog_header_only)
elseif(TARGET spdlog::spdlog)
    set(GALAY_MYSQL_SPDLOG_TARGET spdlog::spdlog)
else()
    message(FATAL_ERROR "spdlog package did not provide spdlog::spdlog or spdlog::spdlog_header_only")
endif()

file(GLOB SOURCES "base/*.cc" "async/*.cc" "protocol/*.cc" "sync/*.cc")

if(NOT TARGET galay-kernel::galay-kernel)
    message(FATAL_ERROR "galay-kernel::galay-kernel target not found. Please check galay-kernel installation and CMake config.")
endif()

add_library(${PROJECT_NAME} ${GALAY_MYSQL_LIBRARY_TYPE} ${SOURCES})

set_target_properties(${PROJECT_NAME} PROPERTIES
    NO_SYSTEM_FROM_IMPORTED ON
    EXPORT_NO_SYSTEM ON
)

target_link_libraries(${PROJECT_NAME}
    PUBLIC galay-kernel::galay-kernel
    PUBLIC OpenSSL::SSL
    PUBLIC OpenSSL::Crypto
    PUBLIC Threads::Threads
    PUBLIC ${GALAY_MYSQL_SPDLOG_TARGET}
)

target_include_directories(${PROJECT_NAME}
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    PUBLIC $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
    PUBLIC $<INSTALL_INTERFACE:include>
)

galay_mysql_apply_cxx(${PROJECT_NAME})

if(GALAY_MYSQL_IMPORT_COMPILATION_ENABLED AND CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
    add_library(${PROJECT_NAME}-modules STATIC)
    target_link_libraries(${PROJECT_NAME}-modules PUBLIC ${PROJECT_NAME})
    target_include_directories(${PROJECT_NAME}-modules
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
            $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
            $<INSTALL_INTERFACE:include>
    )
    target_sources(${PROJECT_NAME}-modules
        PUBLIC
            FILE_SET CXX_MODULES
            BASE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/module
            FILES ${CMAKE_CURRENT_SOURCE_DIR}/module/galay.mysql.cppm
    )
    if(GALAY_MYSQL_IMPORT_COMPILE_OPTIONS)
        target_compile_options(${PROJECT_NAME}-modules PUBLIC ${GALAY_MYSQL_IMPORT_COMPILE_OPTIONS})
    endif()
    set_target_properties(${PROJECT_NAME}-modules PROPERTIES
        CXX_SCAN_FOR_MODULES ON
        OUTPUT_NAME "${PROJECT_NAME}-modules"
        EXPORT_NO_SYSTEM ON
    )
    galay_mysql_apply_cxx(${PROJECT_NAME}-modules)
endif()

# 安装编译完成的库文件
install(TARGETS ${PROJECT_NAME}
    EXPORT galay-mysql-targets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

if(GALAY_MYSQL_IMPORT_COMPILATION_ENABLED AND GALAY_MYSQL_INSTALL_MODULE_INTERFACE AND CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
    install(TARGETS ${PROJECT_NAME}-modules
        EXPORT galay-mysql-targets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        FILE_SET CXX_MODULES DESTINATION include/galay-mysql/module
        INCLUDES DESTINATION include
    )
endif()

set(GALAY_MYSQL_CMAKE_PACKAGE_DIR "lib/cmake/galay-mysql")

# 安装头文件
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
    DESTINATION include/galay-mysql
    FILES_MATCHING
    PATTERN "*.h"
    PATTERN "*.hpp"
    PATTERN "*.inl"
    PATTERN "build" EXCLUDE
    PATTERN "CMakeFiles" EXCLUDE
    PATTERN "*.cc" EXCLUDE
    PATTERN "*.cmake" EXCLUDE
)

if(GALAY_MYSQL_INSTALL_MODULE_INTERFACE AND NOT GALAY_MYSQL_IMPORT_COMPILATION_ENABLED)
    file(GLOB GALAY_MYSQL_MODULE_INTERFACE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/module/*.cppm")
    if(GALAY_MYSQL_MODULE_INTERFACE_FILES)
        install(FILES ${GALAY_MYSQL_MODULE_INTERFACE_FILES}
            DESTINATION include/galay-mysql/module
        )
    endif()
endif()

# 导出CMake配置
install(EXPORT galay-mysql-targets
    FILE galay-mysql-targets.cmake
    NAMESPACE galay-mysql::
    DESTINATION ${GALAY_MYSQL_CMAKE_PACKAGE_DIR}
)

include(CMakePackageConfigHelpers)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/../cmake/galay-mysql-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/galay-mysql-config.cmake"
    INSTALL_DESTINATION ${GALAY_MYSQL_CMAKE_PACKAGE_DIR}
)

set(_galay_mysql_package_version "${PROJECT_VERSION}")
if("${_galay_mysql_package_version}" STREQUAL "")
    set(_galay_mysql_package_version "0.0.0")
endif()

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/galay-mysql-config-version.cmake"
    VERSION ${_galay_mysql_package_version}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/galay-mysql-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/galay-mysql-config-version.cmake"
    DESTINATION ${GALAY_MYSQL_CMAKE_PACKAGE_DIR}
)
