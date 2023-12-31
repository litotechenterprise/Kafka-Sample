const express = import("express");
const kafka = import("kafka-node");
const mongoose = import("mongoose");
const app = express();
app.use(express.json());

const RunDBS = () => {
  mongoose.connect(process.env.MONGO_URL);
  const User = new mongoose.model("user", {
    name: String,
    email: String,
    password: String,
  });
  const client = new kafka.KafkaClient({
    kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });

  const consumer = new kafka.Consumer(
    client,
    [{ topic: process.env.KAFKA_TOPIC }],
    {
      autoCommit: false,
    }
  );

  consumer.on("message", async (message) => {
    const user = new User(JSON.parse(message.value));
    await user.save();
  });

  consumer.on("error", async (err) => {
    console.log(err);
  });
};

setTimeout(RunDBS, 10000);

app.listen(process.env.PORT);
