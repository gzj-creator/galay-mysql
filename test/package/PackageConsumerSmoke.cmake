cmake_minimum_required(VERSION 3.20)

if(NOT DEFINED SOURCE_DIR OR NOT DEFINED BINARY_DIR)
    message(FATAL_ERROR "SOURCE_DIR and BINARY_DIR are required.")
endif()

set(SMOKE_ROOT "${BINARY_DIR}/_package_smoke")
set(INSTALL_PREFIX "${SMOKE_ROOT}/install")
set(CONSUMER_SOURCE_DIR "${SOURCE_DIR}/test/package/consumer")
set(CONSUMER_BUILD_DIR "${SMOKE_ROOT}/consumer-build")

file(REMOVE_RECURSE "${SMOKE_ROOT}")
file(MAKE_DIRECTORY "${SMOKE_ROOT}")

execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${BINARY_DIR}" --prefix "${INSTALL_PREFIX}"
    RESULT_VARIABLE install_rv
    OUTPUT_VARIABLE install_out
    ERROR_VARIABLE install_err
)
if(NOT install_rv EQUAL 0)
    message(FATAL_ERROR
        "Package smoke failed during install step.\n"
        "Command: ${CMAKE_COMMAND} --install ${BINARY_DIR} --prefix ${INSTALL_PREFIX}\n"
        "Output:\n${install_out}\n${install_err}"
    )
endif()

set(_consumer_prefix_path "${INSTALL_PREFIX}")
if(DEFINED DEPENDENCY_PREFIX_HINTS AND NOT "${DEPENDENCY_PREFIX_HINTS}" STREQUAL "")
    string(REPLACE ";" "|" _dep_prefix_log "${DEPENDENCY_PREFIX_HINTS}")
    message(STATUS "Package smoke dependency prefix hints: ${_dep_prefix_log}")
    list(APPEND _consumer_prefix_path ${DEPENDENCY_PREFIX_HINTS})
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -S "${CONSUMER_SOURCE_DIR}"
        -B "${CONSUMER_BUILD_DIR}"
        "-DCMAKE_PREFIX_PATH=${_consumer_prefix_path}"
    RESULT_VARIABLE configure_rv
    OUTPUT_VARIABLE configure_out
    ERROR_VARIABLE configure_err
)
if(NOT configure_rv EQUAL 0)
    message(FATAL_ERROR
        "Package smoke failed during consumer configure step.\n"
        "Output:\n${configure_out}\n${configure_err}"
    )
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" --build "${CONSUMER_BUILD_DIR}" --parallel
    RESULT_VARIABLE build_rv
    OUTPUT_VARIABLE build_out
    ERROR_VARIABLE build_err
)
if(NOT build_rv EQUAL 0)
    message(FATAL_ERROR
        "Package smoke failed during consumer build step.\n"
        "Output:\n${build_out}\n${build_err}"
    )
endif()
