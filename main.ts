import {Kafka, Partitioners} from 'kafkajs';

const BOOTSTRAP_SERVER = 'localhost';
const BOOTSTRAP_PORT = 29092;
const DEFAULT_TOPIC = 'my-topic';

(async () => {
    const kafkaServer = new Kafka(
        {
            clientId: 'testApp',
            brokers: [`${BOOTSTRAP_SERVER}:${BOOTSTRAP_PORT}`]
        });

    const kafkaProducer = kafkaServer.producer({
        createPartitioner: Partitioners.LegacyPartitioner
    });

    await kafkaProducer.connect();
    await kafkaProducer.send({
        topic: DEFAULT_TOPIC,
        messages: [
            { key: 'same', value: 'This is a message' }
        ]
    });

    kafkaProducer.disconnect();

    const kafkaConsumer = kafkaServer.consumer({ groupId: 'my-group-id' });

    await kafkaConsumer.connect();
    await kafkaConsumer.subscribe({ topic: DEFAULT_TOPIC, fromBeginning: true});
    await kafkaConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          console.log({
            value: message?.value?.toString(),
          })
        },
      })
})();