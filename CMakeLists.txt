cmake_minimum_required(VERSION 3.20)
project(sql_parser VERSION 1.0.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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
    #src/mysql/events.cc
    #src/mysql/procedures.cc
    #src/mysql/triggers.cc
)

target_include_directories(sql_parser
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

# Tests target
#add_executable(sql_parser_tests
#tests/lexer_tests.cpp
#tests/parser_tests.cpp
#tests/ast_tests.cpp
#tests/mysql/events_tests.cpp
#tests/mysql/procedures_tests.cpp
#tests/mysql/triggers_tests.cpp
#tests/main.cpp
#)

#target_link_libraries(sql_parser_tests
#PRIVATE
#sql_parser
#GTest::gtest
#GTest::gtest_main
#)

# Enable testing
#enable_testing()
#add_test(NAME sql_parser_tests COMMAND sql_parser_tests)

