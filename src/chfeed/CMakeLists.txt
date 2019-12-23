cmake_minimum_required(VERSION 2.8)
add_compile_options(-std=c++17)

add_executable (chfeed chfeed.cpp) 
target_link_libraries (chfeed LINK_PUBLIC couchit imtjson pthread)
