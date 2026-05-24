cmake_minimum_required(VERSION 3.10)
project(cpppubsub LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define as an interface library (header-only)
add_library(cpppubsub INTERFACE)
add_library(cpppubsub::cpppubsub ALIAS cpppubsub)

target_include_directories(cpppubsub INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(cpppubsub INTERFACE cxx_std_17)

find_package(Threads REQUIRED)
target_link_libraries(cpppubsub INTERFACE Threads::Threads)

# Determine if this is the top-level project
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    set(IS_TOP_LEVEL ON)
else()
    set(IS_TOP_LEVEL OFF)
endif()

# Option to build examples
option(CPPPUBSUB_BUILD_EXAMPLES "Build the example programs" ${IS_TOP_LEVEL})

if(CPPPUBSUB_BUILD_EXAMPLES)
    file(GLOB EXAMPLE_SOURCES "examples/*.cpp")
    foreach(EXAMPLE_SOURCE ${EXAMPLE_SOURCES})
        get_filename_component(EXAMPLE_NAME ${EXAMPLE_SOURCE} NAME_WE)
        add_executable(${EXAMPLE_NAME} ${EXAMPLE_SOURCE})
        target_link_libraries(${EXAMPLE_NAME} PRIVATE cpppubsub::cpppubsub)
    endforeach()
endif()

# Option to build tests
option(CPPPUBSUB_BUILD_TESTS "Build the unit tests" ${IS_TOP_LEVEL})

if(CPPPUBSUB_BUILD_TESTS)
    enable_testing()
    
    include(FetchContent)
    FetchContent_Declare(
      googletest
      URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    )
    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    
    add_subdirectory(tests)
endif()
