const { Kafka, logLevel } = require('kafkajs');

/*
{
    logLevel: logLevel.INFO,
    clientId: 'nodejs-quickstart-kafka-client',
    brokers: [MASTER_BROKER],
    ssl: { rejectUnauthorized: true, ca, key, cert },
}
*/

class KafkaService {
  constructor(connectionConfig) {
    this.kafka = new Kafka(connectionConfig);
  }
}

const TOPIC = 'nodejs-quickstart-kafka-topic';

const MASTER_BROKER = process.env.ServiceURI;
if (!MASTER_BROKER) {
  throw new Error('ServiceURI is not defined. See README.');
}
const key = fs.readFileSync('./service.key');
const cert = fs.readFileSync('./service.cert');
const ca = fs.readFileSync('./ca.pem');

async function getKafka() {
  return new Kafka({
    logLevel: logLevel.INFO,
    clientId: 'nodejs-quickstart-kafka-client',
    brokers: [MASTER_BROKER],
    ssl: { rejectUnauthorized: true, ca, key, cert },
  });
}

// view full docs at https://kafka.js.org/docs/producing
async function testProducer(kafka) {
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: TOPIC,
    messages: [{ value: 'Hello Aiven Kafka, from NodeJS!' }],
  });
  await producer.disconnect();
}

// view full docs at https://kafka.js.org/docs/consuming
async function testConsumer(kafka) {
  let messageCount = 0;
  const consumer = kafka.consumer({
    groupId: 'nodejs-quickstart-kafka-cgroup',
  });

  await consumer.connect();
  try {
    await consumer.subscribe({ topic: TOPIC });
  } catch (error) {
    console.error(
      `\n\n!!!! Ensure that you created "${TOPIC}" in the kafka cluster!!!!\n\n`,
    );
    process.exit(1);
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { offset, value } = message;
      console.log('\nConsumer: Processing message\n');
      console.log({
        topic,
        partition,
        offset,
        value: value.toString(),
      });
      // note that autoCommit is enabled but this ensures offsets are commited
      // disconnecting the consumer
      console.log(
        `\nConsumer: committing offset: ${JSON.stringify({
          topic,
          partition: 0,
          offset: offset,
        })}\n`,
      );
      await consumer.commitOffsets([{ topic, partition: 0, offset: offset }]);
      consumer.disconnect();
    },
  });
}

async function run() {
  console.log(`\nConnecting to cluster@${MASTER_BROKER}\n`);
  const kafka = await getKafka();
  console.log('\nStarting consumer\n');
  await testConsumer(kafka);
  console.log('\nProducer: Sending kafka message\n');
  await testProducer(kafka);
  console.log('\nDone! Cleaning up...\n');
}

run();
