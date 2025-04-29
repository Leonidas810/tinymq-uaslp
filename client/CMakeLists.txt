cmake_minimum_required(VERSION 3.10)
project(tinymq_client VERSION 0.1.0)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable compile commands export
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Find Boost (only need system and thread components for ASIO)
find_package(Boost REQUIRED COMPONENTS system thread)

# Include directories - including parent src directory to reuse packet definitions
include_directories(${Boost_INCLUDE_DIRS} ../src)

# Find pybind11 
find_package(pybind11 REQUIRED)
include_directories(${pybind11_INCLUDE_DIRS})

# Añadir nlohmann/json
include(FetchContent)
FetchContent_Declare(
    json
    URL https://github.com/nlohmann/json/releases/download/v3.11.2/json.tar.xz
)
FetchContent_MakeAvailable(json)
include_directories(${json_SOURCE_DIR}/include)

# Especificar archivos para el módulo Python (sin cli.cpp ni main.cpp)
set(MODULE_SOURCES
    src/bindings.cpp
    src/client.cpp
    # No incluir terminal_ui.cpp porque no existe
)

file(GLOB COMMON_SOURCES "../src/packet.cpp")

# Create Python module using pybind11
pybind11_add_module(tinymq_module ${MODULE_SOURCES} ${COMMON_SOURCES})

# Link libraries para el módulo Python
target_link_libraries(tinymq_module PRIVATE ${Boost_LIBRARIES})

# Ejecutable CLI para el cliente
add_executable(tinymq_cli src/cli.cpp src/client.cpp ../src/packet.cpp)
target_link_libraries(tinymq_cli PRIVATE ${Boost_LIBRARIES})

# Instalar también el script Python
install(FILES src/tinymq_gui.py DESTINATION bin)