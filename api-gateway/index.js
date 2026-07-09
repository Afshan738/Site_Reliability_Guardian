const express = require("express");
const pool = require("./db.js");
const ampq = require("amqplib");
require("dotenv").config();
const app = express();
app.use(express.json());

let channel;
async function connectRabbit() {
  try {
    const connection = await ampq.connect(
      process.env.RABBITMQ_URL || "amqp://localhost",
    );
    channel = await connection.createChannel();
    await channel.assertQueue("monitor_tasks", {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "monitor_tasks_dead",
      },
    });
    console.log("connected to rabbitMQ successfully");
  } catch (e) {
    console.error("rabbitMQ error:", e);
  }
}
connectRabbit();
app.post("/monitors", async (req, res) => {
  const { url, interval } = req.body;
  try {
    const result = await pool.query(
      "INSERT INTO monitors (url, check_interval) VALUES ($1, $2) RETURNING *",
      [url, interval || 60],
    );
    const newMonitor = result.rows[0];
    const task = {
      id: newMonitor.id,
      url: newMonitor.url,
    };
    channel.sendToQueue("monitor_tasks", Buffer.from(JSON.stringify(task)));
    console.log("Task sent to RabbitMQ:", task);

    res.status(201).json({
      message: "Monitor created and task queued!",
      data: newMonitor,
    });
  } catch (e) {
    console.error(e.message);
    res.status(500).json("error in creating Monitor");
  }
});
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API Gateway running on port ${PORT}`);
});
