cmake_minimum_required(VERSION 3.10)
project(tinymq VERSION 0.1.0)

# Enable compile commands export
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Boost (only need system and thread components for ASIO)
find_package(Boost REQUIRED COMPONENTS system thread)

# Include directories Boost
include_directories(${Boost_INCLUDE_DIRS})

# Find pybind11 
find_package(pybind11 REQUIRED)

# Include directories pybind11
include_directories(${pybind11_INCLUDE_DIRS})

# Add all source files
file(GLOB_RECURSE SOURCES "src/*.cpp")

# Create Phython module use pybind11
pybind11_add_module(tinymq_module ${SOURCES})

# Link libraries
target_link_libraries(tinymq_module PRIVATE ${Boost_LIBRARIES})
