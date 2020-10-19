package kafka.sample.clients;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/*
Exercise 1:
Step 2:
1) cd to the project schema-registry-hogwarts-exercise
2) run "mvn clean install"
3) java -cp <path>/schema-registry-101-hogwarts/target/schema-registry-101-hogwarts-1.0-jar-with-dependencies.jar kafka.sample.clients.SimpleConsumer "127.0.0.1:9092" "test_topic" "test_group"

Step 3: 1.4
java -cp <path>/schema-registry-101-hogwarts/target/schema-registry-101-hogwarts-1.0-jar-with-dependencies.jar kafka.sample.clients.SimpleConsumer "127.0.0.1:9092" "test_topic" "test_group" "http://127.0.0.1:7788/api/v1"

 */
public class SimpleConsumer {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class.getName());

        // command line arguments
        final String bootstrapServers = args[0]; //e.g: "127.0.0.1:9092"
        final String topic = args[1]; //e.g: "test_topic"
        final String groupId = args[2]; //e.g: "test_group"


        // create consumer configs
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                }
                consumer.commitSync();
            }
        }
        finally{
            consumer.close();
        }


    }
}
