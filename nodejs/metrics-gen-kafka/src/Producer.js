const { nanoid } = require('nanoid');
const _ = require('lodash');
const DelayGenerator = require('./DelayGenerator');

class Schedule {
  constructor(id, func, options) {
    this.id = id;
    this.func = func;
    this.options = options;
  }
  start() {
    const { maxRuntime, verbose = false } = this.options;
    this.startTime = +new Date();
    if (this.running) {
      return;
    }
    this.running = true;
    const delayGen = new DelayGenerator(this.options);
    const that = this;
    this.runDelayedFunc = () => {
      const duration = +new Date() - that.startTime;
      if (verbose) {
        console.log(
          `${that.id} [EXECUTE] running=${that.running} duration=${duration} maxRuntime=${maxRuntime}`,
        );
      }
      if (!that.running || duration <= maxRuntime) {
        that.func();
        const delay = delayGen.get();
        if (verbose) {
          console.log(`${that.id} [SCHEDULE] ${that.id} for delay=${delay}`);
        }
        setTimeout(that.runDelayedFunc, delay);
      } else {
        that.stop();
      }
    };
    console.log(`${this.id} [STARTING]`);
    this.runDelayedFunc();
  }
  stop() {
    console.log(`${this.id} [STOPPING]`);
    this.running = false;
  }
}

class Producer {
  constructor(kafka) {
    this.kafka = kafka;
    this.scenarios = {};
  }
  async start(options) {
    const { topic, message, verbose = false } = options;

    const producer = this.kafka.producer();
    await producer.connect();
    const id = nanoid(12);
    const send = () => {
      let messages = _.isFunction(message) ? message() : m;
      if (!_.isArray(messages)) {
        messages = [messages];
      }
      if (verbose) {
        console.log('sending', messages);
      }
      producer.send({ topic, messages });
    };
    console.log(`Starting producer scenario with id ${id}`);
    const schedule = new Schedule(id, send, options);
    schedule.start();
    this.scenarios[id] = {
      id,
      options,
      schedule,
      producer,
    };
  }
  stop(id) {
    const scenario = this.scenarios[id];
    if (scenario) {
      try {
        console.log(`Stopping producer scenario with id ${id}`);
        scenario.schedule.stop();
      } catch (error) {
        console.error(error);
      }
      try {
        console.log(`Closing kafka producer for scenario with id ${id}`);
        scenario.producer.disconnect();
      } catch (error) {
        console.error(error);
      }
      delete this.scenarios[id];
    }
  }
}

module.exports = Producer;
