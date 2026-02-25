function(add_mysql_example target_name source_file)
    add_executable(${target_name} ${source_file})
    target_link_libraries(${target_name} PRIVATE ${PROJECT_NAME})
    target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR})
    galay_mysql_apply_cxx(${target_name})
endfunction()

# include 版本
add_mysql_example(E1-AsyncQuery-Include include/E1-AsyncQuery.cc)
add_mysql_example(E2-SyncQuery-Include include/E2-SyncQuery.cc)
add_mysql_example(E3-AsyncPool-Include include/E3-AsyncPool.cc)
add_mysql_example(E4-SyncPreparedTx-Include include/E4-SyncPreparedTx.cc)

# import 版本（按工具链支持自动开启）
if(GALAY_MYSQL_BUILD_MODULE_EXAMPLES_EFFECTIVE)
    add_mysql_example(E1-AsyncQuery-Import import/E1-AsyncQuery.cc)
    add_mysql_example(E2-SyncQuery-Import import/E2-SyncQuery.cc)
    add_mysql_example(E3-AsyncPool-Import import/E3-AsyncPool.cc)
    add_mysql_example(E4-SyncPreparedTx-Import import/E4-SyncPreparedTx.cc)

    if(GALAY_MYSQL_IMPORT_COMPILE_OPTIONS)
        target_compile_options(E1-AsyncQuery-Import PRIVATE ${GALAY_MYSQL_IMPORT_COMPILE_OPTIONS})
        target_compile_options(E2-SyncQuery-Import PRIVATE ${GALAY_MYSQL_IMPORT_COMPILE_OPTIONS})
        target_compile_options(E3-AsyncPool-Import PRIVATE ${GALAY_MYSQL_IMPORT_COMPILE_OPTIONS})
        target_compile_options(E4-SyncPreparedTx-Import PRIVATE ${GALAY_MYSQL_IMPORT_COMPILE_OPTIONS})
    endif()
endif()
