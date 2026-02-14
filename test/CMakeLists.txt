find_package(galay-kernel REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(spdlog REQUIRED)

# T1 - 协议解析测试
add_executable(T1-MysqlProtocol T1-MysqlProtocol.cc)
target_link_libraries(T1-MysqlProtocol PRIVATE ${PROJECT_NAME} galay-kernel::galay-kernel OpenSSL::SSL OpenSSL::Crypto spdlog::spdlog)
target_include_directories(T1-MysqlProtocol PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(T1-MysqlProtocol PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)

# T2 - 认证测试
add_executable(T2-MysqlAuth T2-MysqlAuth.cc)
target_link_libraries(T2-MysqlAuth PRIVATE ${PROJECT_NAME} galay-kernel::galay-kernel OpenSSL::SSL OpenSSL::Crypto spdlog::spdlog)
target_include_directories(T2-MysqlAuth PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(T2-MysqlAuth PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)

# T3 - 异步客户端测试
add_executable(T3-AsyncMysqlClient T3-AsyncMysqlClient.cc)
target_link_libraries(T3-AsyncMysqlClient PRIVATE ${PROJECT_NAME} galay-kernel::galay-kernel OpenSSL::SSL OpenSSL::Crypto spdlog::spdlog)
target_include_directories(T3-AsyncMysqlClient PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(T3-AsyncMysqlClient PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)

# T4 - 同步客户端测试
add_executable(T4-SyncMysqlSession T4-SyncMysqlSession.cc)
target_link_libraries(T4-SyncMysqlSession PRIVATE ${PROJECT_NAME} galay-kernel::galay-kernel OpenSSL::SSL OpenSSL::Crypto spdlog::spdlog)
target_include_directories(T4-SyncMysqlSession PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(T4-SyncMysqlSession PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)

# T5 - 连接池测试
add_executable(T5-ConnectionPool T5-ConnectionPool.cc)
target_link_libraries(T5-ConnectionPool PRIVATE ${PROJECT_NAME} galay-kernel::galay-kernel OpenSSL::SSL OpenSSL::Crypto spdlog::spdlog)
target_include_directories(T5-ConnectionPool PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(T5-ConnectionPool PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)

# T6 - 事务测试
add_executable(T6-Transaction T6-Transaction.cc)
target_link_libraries(T6-Transaction PRIVATE ${PROJECT_NAME} galay-kernel::galay-kernel OpenSSL::SSL OpenSSL::Crypto spdlog::spdlog)
target_include_directories(T6-Transaction PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(T6-Transaction PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)

# T7 - 预处理语句测试
add_executable(T7-PreparedStatement T7-PreparedStatement.cc)
target_link_libraries(T7-PreparedStatement PRIVATE ${PROJECT_NAME} galay-kernel::galay-kernel OpenSSL::SSL OpenSSL::Crypto spdlog::spdlog)
target_include_directories(T7-PreparedStatement PRIVATE ${CMAKE_SOURCE_DIR})
set_target_properties(T7-PreparedStatement PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)
