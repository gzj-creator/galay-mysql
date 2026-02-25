function(add_mysql_benchmark target_name source_file)
    add_executable(${target_name} ${source_file})
    target_link_libraries(${target_name} PRIVATE ${PROJECT_NAME})
    target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR})
    galay_mysql_apply_cxx(${target_name})
endfunction()

add_mysql_benchmark(B1-SyncPressure B1-SyncPressure.cc)
add_mysql_benchmark(B2-AsyncPressure B2-AsyncPressure.cc)

