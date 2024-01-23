package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
public class producerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(producerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        //create Producer Properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i = 0; i < 10; i++) {
            String topic = "demo_java";
            String key = "id_" + i;
            String value = "Hello world " + i;
            //create a Producer Record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key,value);

            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //executes every time a record successfully sent or an exception is thrown
                    if (e == null) {
                        log.info("Key: " + key + "| Partition: " + metadata.partition());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //tell the producer to send all data and block until done -- synchronous
        producer.flush();
        //flush and close the producer
        producer.close();
    }}


