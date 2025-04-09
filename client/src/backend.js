const { spawn } = require('child_process');
const express = require('express');
const WebSocket = require('ws');
const app = express();
const PORT = 3000;

// Middleware para manejar JSON
app.use(express.json());

// Crear un servidor WebSocket
const wss = new WebSocket.Server({ port: 3001 });
console.log('[WebSocket] Servidor WebSocket escuchando en ws://localhost:3001');

// Ejecutar el script Python
const pythonProcess = spawn('python3', ['/home/kris/tinymq-uaslp/client/src/backend.py']);

pythonProcess.stdout.on('data', (data) => {
    const message = data.toString().trim();
    console.log(`[Python] ${message}`);

    // Enviar el mensaje a todos los clientes conectados por WebSocket
    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
});

pythonProcess.stderr.on('data', (data) => {
    console.error(`[Python Error] ${data}`);
});

pythonProcess.on('close', (code) => {
    console.log(`[Python] Proceso terminado con código ${code}`);
});

// Endpoint para verificar el estado del backend
app.get('/status', (req, res) => {
    res.json({ status: 'Backend en ejecución' });
});

// Iniciar el servidor HTTP
app.listen(PORT, () => {
    console.log(`[Backend] Servidor escuchando en http://localhost:${PORT}`);
});

// Manejo de desconexión
process.on('SIGINT', () => {
    console.log('[Backend] Cerrando...');
    pythonProcess.kill('SIGINT');
    process.exit(0);
});