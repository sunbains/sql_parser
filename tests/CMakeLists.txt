message(STATUS "Building tests")

enable_testing()

# Include FetchContent for downloading GTest
include(FetchContent)

# Download and configure Google Test
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.12.1
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Create test executable
add_executable(lexer_tests lexer_tests.cc)
add_executable(lexer_string_tests lexer_string_tests.cc)
add_executable(parser_tests parser_tests.cc)
#add_executable(mysql_extension_tests mysql_extensions_tests.cc)

# Link against our library and GTest
target_link_libraries(lexer_tests PRIVATE GTest::gtest_main GTest::gmock_main ${PROJECT_NAME})
target_link_libraries(lexer_string_tests PRIVATE GTest::gtest_main GTest::gmock_main ${PROJECT_NAME})
target_link_libraries(parser_tests PRIVATE GTest::gtest_main GTest::gmock_main ${PROJECT_NAME})
#target_link_libraries(mysql_extensions_tests PRIVATE GTest::gtest_main GTest::gmock_main ${PROJECT_NAME})

# Include test directory
target_include_directories(lexer_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(lexer_string_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(parser_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${CMAKE_CURRENT_SOURCE_DIR}/include)
#target_include_directories(mysql_extension_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${CMAKE_CURRENT_SOURCE_DIR}/include)

# Discover tests
include(GoogleTest)
gtest_discover_tests(lexer_tests)
gtest_discover_tests(lexer_string_tests)
gtest_discover_tests(parser_tests)
#gtest_discover_tests(mysql_extensions_tests)

# Add custom target for running tests with detailed output
add_custom_target(test_verbose COMMAND ${CMAKE_CTEST_COMMAND} --verbose DEPENDS lexer_tests lexer_string_tests parser_tests)

