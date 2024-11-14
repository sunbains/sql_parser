#set_project_warnings(${PROJECT_NAME})

add_library(${PROJECT_NAME} ast.cc lexer.cc parser.cc)

target_include_directories(${PROJECT_NAME}
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)


# Apply sanitizers if enabled
if(ENABLE_SANITIZERS)
  enable_sanitizers(${PROJECT_NAME})
endif()

# Apply clang-tidy if enabled
if(ENABLE_CLANG_TIDY)
 #set_clang_tidy_options(${PROJECT_NAME})
endif()
