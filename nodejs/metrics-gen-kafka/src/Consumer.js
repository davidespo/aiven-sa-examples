const { nanoid } = require('nanoid');
const _ = require('lodash');
const DelayGenerator = require('./DelayGenerator');

class Consumer {
  constructor(kafka) {
    this.kafka = kafka;
    this.scenarios = {};
  }
  async start(options) {
    const { topic, lagProb, groupId, verbose = false } = options;
    const id = nanoid(12);
    const delayGen = new DelayGenerator(options);
    const scenario = { id, options, running: true };

    const consumer = this.kafka.consumer({ groupId });
    await consumer.connect();

    try {
      await consumer.subscribe({ topic });
    } catch (error) {
      console.error(
        `\n\n!!!! Ensure that you created "${TOPIC}" in the kafka cluster!!!!\n\n`,
        error,
      );
      process.exit(1);
    }

    async function eachMessage() {
      if (scenario.running) {
        const testNum = Math.random();
        if (testNum <= lagProb) {
          const delay = delayGen.get();
          console.log(
            `Lagging consumer with ${testNum}<${lagProb} for ${delay} ms`,
          );
          consumer.pause([{ topic }]);
          setTimeout(() => consumer.resume([{ topic }]), delay);
        }
      } else {
        try {
          await consumer.disconnect();
        } catch (error) {
          console.error(error);
        }
      }
    }
    console.log(`Starting Consumer with id ${id}`);
    consumer.run({ eachMessage });

    this.scenarios[id] = scenario;
  }
  stop(id) {
    const scenario = this.scenarios[id];
    if (scenario) {
      try {
        console.log(`Stopping consumer scenario with id ${id}`);
        scenario.running = false;
      } catch (error) {
        console.error(error);
      }
      delete this.scenarios[id];
    }
  }
}

module.exports = Consumer;
