function(add_mysql_example target_name source_file)
    add_executable(${target_name} ${source_file})
    target_link_libraries(${target_name} PRIVATE ${PROJECT_NAME})
    target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR})
    galay_mysql_apply_cxx(${target_name})
endfunction()

# include 版本（自动扫描 examples/include/*.cc）
file(GLOB include_example_sources CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/include/*.cc"
)
list(SORT include_example_sources)
foreach(example_source IN LISTS include_example_sources)
    get_filename_component(example_name "${example_source}" NAME_WE)
    file(RELATIVE_PATH example_rel "${CMAKE_CURRENT_SOURCE_DIR}" "${example_source}")
    add_mysql_example("${example_name}-Include" "${example_rel}")
endforeach()

# import 版本（按工具链支持自动开启，自动扫描 examples/import/*.cc）
if(GALAY_MYSQL_BUILD_MODULE_EXAMPLES_EFFECTIVE)
    file(GLOB import_example_sources CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/import/*.cc"
    )
    list(SORT import_example_sources)
    foreach(example_source IN LISTS import_example_sources)
        get_filename_component(example_name "${example_source}" NAME_WE)
        file(RELATIVE_PATH example_rel "${CMAKE_CURRENT_SOURCE_DIR}" "${example_source}")
        set(target_name "${example_name}-Import")
        add_mysql_example("${target_name}" "${example_rel}")
        if(GALAY_MYSQL_IMPORT_COMPILE_OPTIONS)
            target_compile_options("${target_name}" PRIVATE ${GALAY_MYSQL_IMPORT_COMPILE_OPTIONS})
        endif()
    endforeach()
endif()
