cmake_minimum_required(VERSION 3.20)
project(galay_mysql_package_consumer_smoke LANGUAGES CXX)

find_package(galay-mysql REQUIRED CONFIG)

add_executable(galay_mysql_package_consumer_smoke main.cc)
target_link_libraries(galay_mysql_package_consumer_smoke PRIVATE galay-mysql::galay-mysql)
set_target_properties(galay_mysql_package_consumer_smoke PROPERTIES
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
