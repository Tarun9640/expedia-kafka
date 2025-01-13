//wait-for-kafka.js

import { Kafka } from 'kafkajs';

const waitForKafka = async () => {
  const kafka = new Kafka({
    clientId: 'health-check',
    brokers: ['kafka:9092'],
  });
  const admin = kafka.admin();

  while (true) {
    try {
      await admin.connect();
      console.log('Connected to Kafka');
      await admin.disconnect();
      break;
    } catch (error) {
      console.error('Failed to connect:', error.message);
      console.log('Waiting for Kafka to be ready...');
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  console.log('Kafka is ready! Starting topic creation...');
  process.exit(0);
};

waitForKafka().catch((error) => {
  console.error('Error in Kafka readiness check:', error.message);
  process.exit(1);
});
