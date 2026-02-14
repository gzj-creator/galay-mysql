function(add_mysql_test target_name source_file)
    add_executable(${target_name} ${source_file})
    target_link_libraries(${target_name} PRIVATE ${PROJECT_NAME})
    target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR})
    galay_mysql_apply_cxx(${target_name})
endfunction()

# T1 - 协议解析测试
add_mysql_test(T1-MysqlProtocol T1-MysqlProtocol.cc)

# T2 - 认证测试
add_mysql_test(T2-MysqlAuth T2-MysqlAuth.cc)

# T3 - 异步客户端测试
add_mysql_test(T3-AsyncMysqlClient T3-AsyncMysqlClient.cc)

# T4 - 同步客户端测试
add_mysql_test(T4-SyncMysqlSession T4-SyncMysqlSession.cc)

# T5 - 连接池测试
add_mysql_test(T5-ConnectionPool T5-ConnectionPool.cc)

# T6 - 事务测试
add_mysql_test(T6-Transaction T6-Transaction.cc)

# T7 - 预处理语句测试
add_mysql_test(T7-PreparedStatement T7-PreparedStatement.cc)
