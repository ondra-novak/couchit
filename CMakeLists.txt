cmake_minimum_required(VERSION 2.8)
project (couchity)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin/)
add_subdirectory (src/couchit)
add_subdirectory (src/tests)
  # The 'test' target runs all but the future tests
  cmake_policy(PUSH)
  if(POLICY CMP0037)
    cmake_policy(SET CMP0037 OLD)
  endif()
add_compile_options(-std=c++11)
add_custom_target( test bin/couchit_test DEPENDS bin/couchit_test)
cmake_policy(POP)  
 