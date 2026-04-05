function(add_mysql_test target_name source_file test_label)
    add_executable(${target_name} ${source_file})
    target_link_libraries(${target_name} PRIVATE ${PROJECT_NAME})
    target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR})
    galay_mysql_apply_cxx(${target_name})
    add_test(NAME ${target_name} COMMAND $<TARGET_FILE:${target_name}>)
    set_tests_properties(${target_name} PROPERTIES
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        LABELS "${test_label}"
    )
    if(test_label STREQUAL "integration")
        set_tests_properties(${target_name} PROPERTIES SKIP_RETURN_CODE 125)
    endif()
endfunction()

# T1 - 协议解析测试
add_mysql_test(T1-MysqlProtocol T1-mysql_protocol.cc unit)

# T2 - 认证测试
add_mysql_test(T2-MysqlAuth T2-mysql_auth.cc unit)

# T3 - 异步客户端测试
add_mysql_test(T3-AsyncMysqlClient T3-async_mysql_client.cc integration)

# T4 - 同步客户端测试
add_mysql_test(T4-SyncMysqlClient T4-sync_mysql_client.cc integration)

# T5 - 连接池测试
add_mysql_test(T5-ConnectionPool T5-connection_pool.cc integration)

# T6 - 事务测试
add_mysql_test(T6-Transaction T6-transaction.cc integration)

# T7 - 预处理语句测试
add_mysql_test(T7-PreparedStatement T7-prepared_statement.cc integration)

# T8 - Awaitable surface test
add_mysql_test(T8-MysqlAwaitableSurface T8-mysql_awaitable_surface.cc unit)

# T9 - Config env override surface
add_mysql_test(T9-ConfigEnvSurface T9-config_env_surface.cc unit)

add_test(
    NAME PackageConfig.ConsumerSmoke
    COMMAND ${CMAKE_COMMAND}
        -DBINARY_DIR=${CMAKE_BINARY_DIR}
        -DSOURCE_DIR=${CMAKE_SOURCE_DIR}
        -DDEPENDENCY_PREFIX_HINTS=${GALAY_MYSQL_LOCAL_PREFIX_HINTS}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/package/PackageConsumerSmoke.cmake
)
set_tests_properties(PackageConfig.ConsumerSmoke PROPERTIES
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    LABELS "package"
)
