cmake_minimum_required(VERSION 3.10)
project(tinymq_client VERSION 0.1.0)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Boost (only need system and thread components for ASIO)
find_package(Boost REQUIRED COMPONENTS system thread)

# Include directories - including parent src directory to reuse packet definitions
include_directories(${Boost_INCLUDE_DIRS} ../src)

# Find pybind11 
find_package(pybind11 REQUIRED)

# Include directories pybind11
include_directories(${pybind11_INCLUDE_DIRS})

# Add all source files
file(GLOB_RECURSE CLIENT_SOURCES "src/*.cpp")
file(GLOB COMMON_SOURCES "../src/packet.cpp")

# Create Python module using pybind11
pybind11_add_module(tinymq_module ${CLIENT_SOURCES} ${COMMON_SOURCES})

# Link libraries
target_link_libraries(tinymq_module PRIVATE ${Boost_LIBRARIES})