find_package(galay-kernel REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(spdlog REQUIRED)
find_path(SPDLOG_INCLUDE_DIR spdlog/spdlog.h REQUIRED)

include_directories(${OPENSSL_INCLUDE_DIR})

file(GLOB SOURCES "base/*.cc" "async/*.cc" "protocol/*.cc" "sync/*.cc")

if(NOT TARGET galay-kernel::galay-kernel)
    message(FATAL_ERROR "galay-kernel::galay-kernel target not found. Please check galay-kernel installation and CMake config.")
endif()

add_library(${PROJECT_NAME} ${GALAY_MYSQL_LIBRARY_TYPE} ${SOURCES})

target_link_libraries(${PROJECT_NAME}
    PUBLIC galay-kernel::galay-kernel
    PUBLIC OpenSSL::SSL
    PUBLIC OpenSSL::Crypto
    PUBLIC pthread
)

target_include_directories(${PROJECT_NAME}
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    PUBLIC $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
    PUBLIC $<INSTALL_INTERFACE:include>
    PUBLIC ${OPENSSL_INCLUDE_DIR}
    PUBLIC ${SPDLOG_INCLUDE_DIR}
)

galay_mysql_apply_cxx(${PROJECT_NAME})

if(GALAY_MYSQL_IMPORT_COMPILATION_ENABLED AND CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
    target_sources(${PROJECT_NAME}
        PUBLIC
            FILE_SET CXX_MODULES
            FILES ${CMAKE_CURRENT_SOURCE_DIR}/module/galay.mysql.cppm
    )
    if(GALAY_MYSQL_IMPORT_COMPILE_OPTIONS)
        target_compile_options(${PROJECT_NAME} PUBLIC ${GALAY_MYSQL_IMPORT_COMPILE_OPTIONS})
    endif()
endif()

# 安装编译完成的库文件
if(GALAY_MYSQL_IMPORT_COMPILATION_ENABLED AND GALAY_MYSQL_INSTALL_MODULE_INTERFACE AND CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
    install(TARGETS ${PROJECT_NAME}
        EXPORT galay-mysql-targets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        INCLUDES DESTINATION include
        FILE_SET CXX_MODULES DESTINATION include/galay-mysql/module
    )
else()
    install(TARGETS ${PROJECT_NAME}
        EXPORT galay-mysql-targets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        INCLUDES DESTINATION include
    )
endif()

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
    FILE GalayMysqlTargets.cmake
    NAMESPACE galay-mysql::
    DESTINATION lib/cmake/galay-mysql
)

include(CMakePackageConfigHelpers)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/../cmake/GalayMysqlConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/GalayMysqlConfig.cmake"
    INSTALL_DESTINATION lib/cmake/galay-mysql
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/GalayMysqlConfig.cmake"
    DESTINATION lib/cmake/galay-mysql
)
