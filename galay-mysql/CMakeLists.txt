find_package(galay-kernel REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(spdlog REQUIRED)

include_directories(${OPENSSL_INCLUDE_DIR})

file(GLOB SOURCES "base/*.cc" "async/*.cc" "protocol/*.cc" "sync/*.cc")

if(NOT TARGET galay-kernel::galay-kernel)
    message(FATAL_ERROR "galay-kernel::galay-kernel target not found. Please check galay-kernel installation and CMake config.")
endif()

add_library(${PROJECT_NAME} SHARED ${SOURCES})

target_link_libraries(${PROJECT_NAME}
    PUBLIC galay-kernel::galay-kernel
    PUBLIC OpenSSL::SSL
    PUBLIC OpenSSL::Crypto
    PUBLIC spdlog::spdlog
    PUBLIC pthread
)

target_include_directories(${PROJECT_NAME}
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    PUBLIC $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
    PUBLIC $<INSTALL_INTERFACE:include>
    PUBLIC ${OPENSSL_INCLUDE_DIR}
)

set_target_properties(${PROJECT_NAME} PROPERTIES
        CXX_STANDARD 23
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
)

# 安装编译完成的库文件
install(TARGETS ${PROJECT_NAME}
    EXPORT galay-mysql-targets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

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
