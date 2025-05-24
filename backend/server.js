import WebSocket, { WebSocketServer } from "ws";
import { MongoClient } from "mongodb";

const MONGO_URI = "mongodb://localhost:27017";
const DB_NAME = "crypto_db";
const COLLECTION_NAME = "prices";

const mongoClient = new MongoClient(MONGO_URI);

async function getDataInRange(hours) {
  const db = mongoClient.db(DB_NAME);
  const collection = db.collection(COLLECTION_NAME);

  const since = new Date(Date.now() - hours * 60 * 60 * 1000);

  const cursor = collection.find({ timestamp: { $gte: since } }).sort({ timestamp: 1 });
  return await cursor.toArray();
}

async function startServer() {
  await mongoClient.connect();

  const wss = new WebSocketServer({ port: 8000 });

  wss.on("connection", (ws) => {
    // Default to 24h data
    let currentRange = 24;

    async function sendData() {
      const data = await getDataInRange(currentRange);
      ws.send(JSON.stringify({ type: "history", data }));
    }

    sendData();

    ws.on("message", async (message) => {
      try {
        const msg = JSON.parse(message);

        if (msg.type === "request_range") {
          if (msg.range === "1h") currentRange = 1;
          else currentRange = 24;

          await sendData();
        }
      } catch (err) {
        console.error("Failed to handle message:", err);
      }
    });

    // You can also add code here to push new updates in real time.
  });

  console.log("WebSocket server running on ws://localhost:8000");
}

startServer();
