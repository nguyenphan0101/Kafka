package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producer_demo {

    private static final Logger log = LoggerFactory.getLogger(producer_demo.class.getSimpleName());
    public static void main(String[] args){
        log.info("I am a Kafka Producer!");

        //create Producer Properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create a Producer Record
        ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_java", "Day la task 1");

        //send data
        producer.send(producerRecord);

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}
