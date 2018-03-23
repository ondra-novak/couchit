cmake_minimum_required(VERSION 2.8) 
add_compile_options(-std=c++14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FORCE_INLINES")
file(GLOB couchit_SRC "*.cpp" "minihttp/netio.cpp" "minihttp/httpclient.cpp" "minihttp/consts.cpp")
file(GLOB couchit_HDR "*.h" "*.tcc")
file(GLOB couchit_http_HDR "minihttp/*.h")
add_library (couchit ${couchit_SRC})

install(TARGETS couchit
        DESTINATION "lib"
        ) 
INSTALL(FILES ${couchit_HDR}
        DESTINATION "include/couchit"
        )  
INSTALL(FILES ${couchit_http_HDR}
        DESTINATION "include/couchit/minihttp"
        )  