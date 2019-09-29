const { Kafka } = require('kafkajs');

const host = process.env.HOST_IP;

const kafka = new Kafka({
  clientId: 'first-app',
  brokers: [`${host}:9092`],
});

const topic = 'topic-test';
const producer = kafka.producer();

const getRandomInit = () => Math.round(Math.random(10) * 1000);

const createMessage = num => ({
  key: `key-${num}`,
  value: JSON.stringify({ data: `value-${num}-${new Date().toISOString()}` }),
});

const sendMessage = () => {
  return producer
    .send({
      topic,
      messages: Array(getRandomInit())
        .fill()
        .map(_ => createMessage(getRandomInit())),
    })
    .then(console.log)
    .catch(e => console.error(`[example/producer] ${e.message}`, e));
};

const run = async () => {
  await producer.connect();
  setInterval(sendMessage, 3000);
};

run().catch(e => console.error(`[example/producer] ${e.message}`, e));

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`);
      await producer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});
