#include "client.h"
#include "terminal_ui.h"
#include <iostream>
#include <string>
#include <cstdlib>

int main(int argc, char *argv[])
{
    // Invocar el script Python para la interfaz gráfica
    std::cout << "Lanzando interfaz gráfica de TinyMQ..." << std::endl;
    system("PYTHONPATH=$(pwd) python3 ../src/tinymq_gui.py");

    return 0;
}