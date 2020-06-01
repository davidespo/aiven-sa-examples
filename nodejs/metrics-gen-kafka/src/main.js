const { getKafka, readCreds } = require('./kafka');
const { nanoid } = require('nanoid');
const Producer = require('./Producer');
const Consumer = require('./Consumer');
const { DURATION } = require('./DelayGenerator');

const TOPIC_STATUS = 'device_status_v2';
const TOPIC_METRICS = 'raw_metrics_v1';
const keys = ['d-000132', 'd-000074', 'd-008351'];

const kafka = getKafka(
  'kafka-e271828-david-demo.aivencloud.com:24590',
  readCreds(),
);
// const kafka = getKafka('localhost:9092');

const sensors = {
  TEMP: () => Math.random() * 100 + 40,
  'ACC-X': () => Math.random(),
  'ACC-Y': () => Math.random(),
  HUMID: () => Math.random(),
};

const status = (key) => ({
  key: `${key}`,
  value: JSON.stringify({
    key,
    ts: new Date().toISOString(),
    status: Math.random() < 0.01 ? 'DOWN' : 'UP',
  }),
});

const metric = (key, sensor, value) => ({
  key: `"${nanoid(10)}"`,
  value: JSON.stringify({ key, ts: new Date().toISOString(), sensor, value }),
});

new Producer(kafka).start({
  topic: TOPIC_METRICS,
  message: () => {
    const messages = [];
    keys.forEach((key) => {
      Object.keys(sensors).forEach((sensorKey) => {
        messages.push(metric(key, sensorKey, sensors[sensorKey]()));
      });
    });
    return messages;
  },
  delayMs: {
    strategy: 'STICKY',
    min: DURATION.seconds(30),
    max: DURATION.minutes(4),
    ranges: [
      { min: 10, max: 50, key: 'SPIKE' },
      { min: 50, max: 100, key: 'FAST' },
      { min: 100, max: 250, key: 'AVERAGE' },
      { min: 250, max: 1000, key: 'SLOW' },
    ],
  },
  maxRuntime: DURATION.minutes(30),
  // verbose: true,
});

new Producer(kafka).start({
  topic: TOPIC_STATUS,
  message: () => keys.map((key) => status(key)),
  delayMs: [100, 1000],
  maxRuntime: DURATION.minutes(30),
  // verbose: true,
});

new Consumer(kafka).start({
  topic: TOPIC_METRICS,
  groupId: `avn-demo-metrics-cgroup`,
  lagProb: 0.00001,
  delayMs: DURATION.minutes(1),
  // verbose: true,
});

new Consumer(kafka).start({
  topic: TOPIC_STATUS,
  groupId: `avn-demo-health-cgroup`,
  lagProb: 0.00001,
  delayMs: DURATION.minutes(1),
  // verbose: true,
});
