cmake_minimum_required(VERSION 2.8)
project (couchity)

include_directories(BEFORE src/imtjson/src src)
add_compile_options(-std=c++17)
add_compile_options(-Wall -Wno-noexcept-type)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin/)
add_subdirectory (src/couchit)
add_subdirectory (src/imtjson/src/imtjson EXCLUDE_FROM_ALL)
add_subdirectory (src/tests)
add_subdirectory (src/chfeed)
add_compile_options(-std=c++11)
add_custom_target( test bin/couchit_test DEPENDS bin/couchit_test)
 