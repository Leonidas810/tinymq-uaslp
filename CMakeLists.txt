cmake_minimum_required(VERSION 3.10)
project(tinymq VERSION 0.1.0)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Export compile commands (for tools like clangd)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ========================
# Find Boost
# ========================
find_package(Boost REQUIRED COMPONENTS system thread)
include_directories(${Boost_INCLUDE_DIRS})

# ========================
# Include nlohmann/json (header-only)
# ========================
include(FetchContent)
FetchContent_Declare(
    json
    URL https://github.com/nlohmann/json/releases/download/v3.11.2/json.tar.xz
)
FetchContent_MakeAvailable(json)

# ========================
# Find libpqxx
# ========================
find_package(PkgConfig REQUIRED)
pkg_check_modules(PQXX REQUIRED libpqxx)
include_directories(${PQXX_INCLUDE_DIRS})
link_directories(${PQXX_LIBRARY_DIRS})

# ========================
# Source files, excluding bindings.cpp
# ========================
file(GLOB_RECURSE ALL_SOURCES "src/*.cpp")

set(SOURCES "")
foreach(source_file ${ALL_SOURCES})
    if (NOT source_file MATCHES ".*bindings\\.cpp$")
        list(APPEND SOURCES ${source_file})
    endif()
endforeach()

# ========================
# Create executable
# ========================
add_executable(tinymq_broker ${SOURCES})

# ========================
# Link libraries
# ========================
target_link_libraries(tinymq_broker
    PRIVATE
    ${Boost_LIBRARIES}
    nlohmann_json::nlohmann_json
    ${PQXX_LIBRARIES}
)

# ========================
# Install target (optional)
# ========================
install(TARGETS tinymq_broker DESTINATION bin)
