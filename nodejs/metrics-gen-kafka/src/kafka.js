const fs = require('fs');
const { Kafka, logLevel } = require('kafkajs');

module.exports = {
  readCreds: (path = './') => ({
    key: fs.readFileSync(`${path}service.key`),
    cert: fs.readFileSync(`${path}service.cert`),
    ca: fs.readFileSync(`${path}ca.pem`),
  }),
  getKafka: (broker, creds) => {
    let ssl;
    if (!!creds) {
      const { ca, key, cert } = creds;
      ssl = { rejectUnauthorized: true, ca, key, cert };
    }
    return new Kafka({
      logLevel: logLevel.INFO,
      clientId: 'nodejs-metrics-gen-kafka-client',
      brokers: [broker],
      ssl,
    });
  },
};
