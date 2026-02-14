cmake_minimum_required(VERSION 3.20)
project(galay-mysql)

message(STATUS "CXX_STANDARD: ${CMAKE_CXX_STANDARD}")

add_subdirectory(galay-mysql)
add_subdirectory(test)
