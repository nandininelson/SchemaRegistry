package com.cloudera.kafka;

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

public class SimpleConsumer {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class.getName());

        // Section 1: command line arguments
        // final String bootstrapServers = args[0]; //e.g: "127.0.0.1:9092"
        final String bootstrapServers = args[0]; //e.g: "127.0.0.1:9092"
        final String topic = args[1]; //e.g: "test_topic"
        final String groupId = args[2]; //e.g: "test_group"

        // Section 2: create consumer configs
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // Section 3: create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // Section 4: subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        // Section 5: poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                }

                // commit offsets
                consumer.commitSync();
            }
        }
        finally{
            // close consumer
            consumer.close();
        }


    }
}
