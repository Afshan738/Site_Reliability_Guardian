const pool = require("./db.js");
const amqp = require("amqplib");
require("dotenv").config();

async function startScheduler() {
  try {
    console.log("Connecting to RabbitMQ at 127.0.0.1...");
    const connection = await amqp.connect(
      process.env.RABBITMQ_URL || "amqp://guest:guest@127.0.0.1:5672",
    );

    const channel = await connection.createChannel();
    await channel.assertQueue("monitor_tasks", {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "monitor_tasks_dead",
      },
    });

    console.log(" Scheduler is connected to RabbitMQ ....");
    setInterval(async () => {
      console.log("\n Searching for overdue monitors...");

      try {
        const results = await pool.query(`
          SELECT id, url FROM monitors 
          WHERE last_checked IS NULL 
          OR last_checked < NOW() - (check_interval * INTERVAL '1 second')
        `);

        console.log(`Found ${results.rows.length} sites that need monitoring.`);

        results.rows.forEach((monitor) => {
          const task = { id: monitor.id, url: monitor.url };
          channel.sendToQueue(
            "monitor_tasks",
            Buffer.from(JSON.stringify(task)),
          );

          console.log(`  Scheduled: ${monitor.url}`);
        });
      } catch (dbErr) {
        console.error(" Database Query Error:", dbErr.message);
      }
    }, 10000);
  } catch (error) {
    console.error(" Failed to connect to RabbitMQ:", error.message);
    console.log("Retrying connection in 5 seconds...");
    setTimeout(startScheduler, 5000);
  }
}
startScheduler();
