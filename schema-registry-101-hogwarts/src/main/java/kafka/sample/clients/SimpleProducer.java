package kafka.sample.clients;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/*
Exercise 1:
Step 2:
1) cd to the project schema-registry-hogwarts-exercise
2) run "mvn clean install"
3) java -cp <path>/schema-registry-101-hogwarts/target/schema-registry-101-hogwarts-1.0-jar-with-dependencies.jar kafka.sample.clients.SimpleProducer "127.0.0.1:9092" "test_topic"

Step 3: 2.6
java -cp <path>/schema-registry-101-hogwarts/target/schema-registry-101-hogwarts-1.0-jar-with-dependencies.jar kafka.sample.clients.SimpleProducer "127.0.0.1:9092" "test_topic" "http://127.0.0.1:7788/api/v1" "sr101"


 */
public class SimpleProducer {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(SimpleProducer.class.getName());

        // command line arguments
        final String bootstrapServers = args[0]; //e.g: "127.0.0.1:9092"
        final String topic = args[1]; //e.g: "test_topic"


        // create Producer props
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // create a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topic, "sample data");

        // send data - asynchronous
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }
        });

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();

    }
}
