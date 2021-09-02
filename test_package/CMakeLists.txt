cmake_minimum_required(VERSION 3.1)
project(PackageTest CXX)


include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
include(${CMAKE_BINARY_DIR}/conan_paths.cmake OPTIONAL)
conan_basic_setup()

find_package(EnTT REQUIRED)
find_package(Edyn REQUIRED)

add_executable(example example.cpp)
target_compile_features(example PUBLIC cxx_std_17)
target_link_libraries(example Edyn::Edyn EnTT::EnTT)
#target_include_directories(example PUBLIC ${CONAN_INCLUDE_DIRS})

# CTest is a testing tool that can be used to test your project.
# enable_testing()
# add_test(NAME example
#          WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
#          COMMAND example)
