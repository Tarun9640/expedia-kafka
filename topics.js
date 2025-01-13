import {Kafka} from 'kafkajs';

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['kafka:9092'],  // Changed to use internal Docker network name
    connectionTimeout: 8000,
    retry: {
        initialRetryTime: 300,
        retries: 5
    }
});

const admin = kafka.admin();

const createTopics = async () => {
    try {
        console.log('Connecting to Kafka...');
        await admin.connect();
        console.log('Successfully connected to Kafka');

        // List existing topics before creation
        const existingTopics = await admin.listTopics();
        console.log('Existing topics:', existingTopics);

        const topicsToCreate = [
            {
                topic: 'vcc_payments',
                numPartitions: 1,
                replicationFactor: 1,
            },
            {
                topic: 'offline_payments',
                numPartitions: 1,
                replicationFactor: 1,
            },
        ];

        console.log('Creating topics...');
        await admin.createTopics({
            topics: topicsToCreate,
            waitForLeaders: true,
            timeout: 10000,
        });

        // Wait for a moment to ensure topics are fully created
        await new Promise(resolve => setTimeout(resolve, 2000));

        // Verify topics after creation
        const finalTopics = await admin.listTopics();
        console.log('Final topics list:', finalTopics);

    } catch (error) {
        console.error('Error creating topics:', error);
        throw error;
    } finally {
        try {
            await admin.disconnect();
            console.log('Disconnected from Kafka');
        } catch (error) {
            console.error('Error disconnecting:', error);
        }
    }
};

createTopics()
    .then(() => {
        console.log('Operation completed successfully');
        process.exit(0);
    })
    .catch(error => {
        console.error('Operation failed:', error);
        process.exit(1);
    });