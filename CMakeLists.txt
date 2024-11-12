cmake_minimum_required(VERSION 3.20)
project(sql_parser VERSION 1.0.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

SET (EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)

# Build options
option(SQL_PARSER_BUILD_TESTS "Build test suite" ON)

# Dependencies
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.12.1
)
FetchContent_MakeAvailable(googletest)

# Compiler options
add_compile_options(
  -Wall
  -Wextra
  -Wpedantic
  -Werror
  -fno-omit-frame-pointer
  $<$<CONFIG:Debug>:-fsanitize=address,undefined>
  $<$<CONFIG:Debug>:-fno-optimize-sibling-calls>
)

add_link_options(
  $<$<CONFIG:Debug>:-fsanitize=address,undefined>
)

# Library target
add_library(sql_parser
  src/lexer.cc
  src/parser.cc
  #src/ast.ccc
  #src/mysql_events.cc
  #src/mysql_procedures.cc
  #src/mysql_triggers.cc
)

target_include_directories(sql_parser PUBLIC ${PROJECT_SOURCE_DIR}/include)

# Tests
if(SQL_PARSER_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

