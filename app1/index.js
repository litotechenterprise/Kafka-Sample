const express = import("express");
const kafka = import("kafka-node");
const app = express();
const sequelize = import("sequelize");
app.use(express.json());

const RunDBS = () => {
  const db = new sequelize(process.env.POSTGRES_URL);
  const USER = db.define("user", {
    name: sequelize.STRING,
    email: sequelize.STRING,
    password: sequelize.STRING,
  });
  db.sync({ force: true });
  const client = new kafka.KafkaClient({
    kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });
  const producer = new kafka.Producer(client);
  producer.on("ready", async () => {
    app.post("/", (req, res) => {
      producer.send(
        [
          {
            topic: process.env.KAFKA_TOPIC,
            messages: JSON.stringify(req.body),
          },
        ],
        async (err, data) => {
          if (err) {
            console.log(err);
          } else {
            await USER.create(req.body);
            res.send({ data, request: req.body });
          }
        }
      );
    });
  });
};

setTimeout(RunDBS, 10000);

app.listen(process.env.PORT);
