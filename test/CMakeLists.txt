set(GALAY_MYSQL_TEST_SKIP_RETURN_CODE 125)

function(add_mysql_test target_name source_file)
    cmake_parse_arguments(ARG "INTEGRATION" "" "LABELS" ${ARGN})
    add_executable(${target_name} ${source_file})
    target_link_libraries(${target_name} PRIVATE ${PROJECT_NAME})
    target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR})
    galay_mysql_apply_cxx(${target_name})
    add_test(NAME ${target_name} COMMAND $<TARGET_FILE:${target_name}>)

    set(test_labels mysql)
    if(ARG_INTEGRATION)
        list(APPEND test_labels integration requires-mysql)
        set_tests_properties(${target_name} PROPERTIES SKIP_RETURN_CODE ${GALAY_MYSQL_TEST_SKIP_RETURN_CODE})
    else()
        list(APPEND test_labels unit)
    endif()

    if(ARG_LABELS)
        list(APPEND test_labels ${ARG_LABELS})
    endif()

    set_tests_properties(${target_name} PROPERTIES
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        LABELS "${test_labels}"
        TIMEOUT 30
    )
endfunction()

# T0 - 测试配置契约
add_mysql_test(T0-ConfigContract T0-config_contract.cc)

# T1 - 协议解析测试
add_mysql_test(T1-MysqlProtocol T1-mysql_protocol.cc)

# T2 - 认证测试
add_mysql_test(T2-MysqlAuth T2-mysql_auth.cc)

# T3 - 异步客户端测试
add_mysql_test(T3-AsyncMysqlClient T3-async_mysql_client.cc INTEGRATION)

# T4 - 同步客户端测试
add_mysql_test(T4-SyncMysqlClient T4-sync_mysql_client.cc INTEGRATION)

# T5 - 连接池测试
add_mysql_test(T5-ConnectionPool T5-connection_pool.cc INTEGRATION)

# T6 - 事务测试
add_mysql_test(T6-Transaction T6-Transaction.cc INTEGRATION)

# T7 - 预处理语句测试
add_mysql_test(T7-PreparedStatement T7-prepared_statement.cc INTEGRATION)

# T8 - Awaitable surface test
add_mysql_test(T8-MysqlAwaitableSurface T8-mysql_awaitable_surface.cc)

# T9 - Config env override surface
add_mysql_test(T9-ConfigEnvSurface T9-config_env_surface.cc)

add_test(
    NAME PackageConfig.ConsumerSmoke
    COMMAND
        ${CMAKE_COMMAND}
        -DGALAY_MYSQL_SOURCE_DIR=${CMAKE_SOURCE_DIR}
        -DGALAY_MYSQL_BINARY_DIR=${CMAKE_BINARY_DIR}
        -DGALAY_MYSQL_CMAKE_GENERATOR=${CMAKE_GENERATOR}
        -DGALAY_MYSQL_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DGALAY_MYSQL_CXX_STANDARD=${GALAY_MYSQL_CXX_STANDARD}
        -DGALAY_MYSQL_PACKAGE_CMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
        -DGALAY_MYSQL_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DGALAY_MYSQL_INSTALL_CONFIG=$<CONFIG>
        -P ${CMAKE_SOURCE_DIR}/test/package/PackageConsumerSmoke.cmake
)
set_tests_properties(PackageConfig.ConsumerSmoke PROPERTIES
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    LABELS "mysql;package;consumer"
    TIMEOUT 120
)
