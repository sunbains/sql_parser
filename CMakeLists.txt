cmake_minimum_required(VERSION 3.20)

project(sql_parser VERSION 1.0.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

SET (EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)

option(SQL_PARSER_BUILD_TESTS "Build test suite" ON)

include(cmake/CompilerOptions.cmake)

add_link_options($<$<CONFIG:Debug>:-fsanitize=address,undefined>)

add_subdirectory(src)

#add_library(${PROJECT_NAME ${SOME_ADDITIONAL_SOURCES}}
target_include_directories(${PROJECT_NAME} PUBLIC ${PROJECT_SOURCE_DIR}/include)

if(SQL_PARSER_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

