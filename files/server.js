const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = 3000;

// Simple HTTP server to serve HTML files
const httpServer = http.createServer((req, res) => {
  let filePath = "";
  if (req.url === "/" || req.url === "/host") {
    filePath = path.join(__dirname, "host.html");
  } else if (req.url === "/client") {
    filePath = path.join(__dirname, "client.html");
  } else {
    res.writeHead(404);
    res.end("Not found");
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(500);
      res.end("Error loading file");
      return;
    }
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(data);
  });
});

// WebSocket server on top of HTTP server
const wss = new WebSocketServer({ server: httpServer });

let hostSocket = null;
const clients = new Map(); // clientId -> { ws, name }
let nextClientId = 1;

function broadcast(data, excludeWs = null) {
  const msg = JSON.stringify(data);
  wss.clients.forEach((ws) => {
    if (ws !== excludeWs && ws.readyState === ws.OPEN) {
      ws.send(msg);
    }
  });
}

wss.on("connection", (ws) => {
  let role = null;
  let clientId = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    switch (msg.type) {
      // ── Role registration ──────────────────────────────────────────
      case "register-host":
        role = "host";
        hostSocket = ws;
        console.log("[HOST] Connected");
        // Send existing clients list
        const existingClients = [];
        clients.forEach((c, id) => {
          existingClients.push({ id, name: c.name });
        });
        ws.send(JSON.stringify({ type: "client-list", clients: existingClients }));
        break;

      case "register-client":
        role = "client";
        clientId = nextClientId++;
        const clientName = msg.name || `Client ${clientId}`;
        clients.set(clientId, { ws, name: clientName });
        console.log(`[CLIENT #${clientId}] Connected: ${clientName}`);
        ws.send(JSON.stringify({ type: "registered", id: clientId, name: clientName }));
        // Notify host
        if (hostSocket && hostSocket.readyState === hostSocket.OPEN) {
          hostSocket.send(JSON.stringify({ type: "client-joined", id: clientId, name: clientName }));
        }
        break;

      // ── WebRTC Signaling ───────────────────────────────────────────
      case "offer":
        // Client → Host
        if (role === "client" && hostSocket && hostSocket.readyState === hostSocket.OPEN) {
          hostSocket.send(JSON.stringify({ type: "offer", from: clientId, sdp: msg.sdp }));
        }
        break;

      case "answer":
        // Host → specific Client
        if (role === "host") {
          const target = clients.get(msg.to);
          if (target && target.ws.readyState === target.ws.OPEN) {
            target.ws.send(JSON.stringify({ type: "answer", sdp: msg.sdp }));
          }
        }
        break;

      case "ice-candidate":
        if (role === "client" && hostSocket && hostSocket.readyState === hostSocket.OPEN) {
          hostSocket.send(JSON.stringify({ type: "ice-candidate", from: clientId, candidate: msg.candidate }));
        } else if (role === "host") {
          const target = clients.get(msg.to);
          if (target && target.ws.readyState === target.ws.OPEN) {
            target.ws.send(JSON.stringify({ type: "ice-candidate", candidate: msg.candidate }));
          }
        }
        break;

      // ── Host requests a stream from a specific client ──────────────
      case "request-stream":
        if (role === "host") {
          const target = clients.get(msg.to);
          if (target && target.ws.readyState === target.ws.OPEN) {
            target.ws.send(JSON.stringify({ type: "request-stream" }));
          }
        }
        break;
    }
  });

  ws.on("close", () => {
    if (role === "host") {
      console.log("[HOST] Disconnected");
      hostSocket = null;
    } else if (role === "client" && clientId !== null) {
      console.log(`[CLIENT #${clientId}] Disconnected`);
      clients.delete(clientId);
      if (hostSocket && hostSocket.readyState === hostSocket.OPEN) {
        hostSocket.send(JSON.stringify({ type: "client-left", id: clientId }));
      }
    }
  });

  ws.on("error", (err) => console.error("WS error:", err.message));
});

httpServer.listen(PORT, () => {
  console.log(`\n✅ Camera Monitor Server running`);
  console.log(`   Host (admin):  http://localhost:${PORT}/host`);
  console.log(`   Client:        http://localhost:${PORT}/client\n`);
});
