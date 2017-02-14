cmake_minimum_required(VERSION 2.8) 
add_compile_options(-std=c++11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FORCE_INLINES")
file(GLOB couchity_SRC "*.cpp" "minihttp/netio.cpp" "minihttp/httpclient.cpp")
file(GLOB couchity_HDR "*.h" "*.tcc" "minihttp/*.h" "btree/*.h")
add_library (couchity ${couchity_SRC})

install(TARGETS couchity
        DESTINATION "lib"
        ) 
INSTALL(FILES ${imtjson_HDR}
        DESTINATION "include/couchity"
        )  