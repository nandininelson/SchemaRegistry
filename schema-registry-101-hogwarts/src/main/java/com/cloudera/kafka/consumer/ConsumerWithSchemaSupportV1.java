package com.cloudera.kafka.consumer;

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

public class ConsumerWithSchemaSupportV1 {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ConsumerWithSchemaSupportV1.class.getName());

        // Section 1: command line arguments
        // Section 1.0 : Client Mandatory configs
        final String bootstrapServers = args[0]; //e.g: "127.0.0.1:9092"
        final String topic = args[1]; //e.g: "test_topic"
        final String groupId = args[2]; //e.g: "test_group"
        // TODO: Section 1.1 : Schema Registry Configs

        // TODO: Section 1.11 : Security Configs

        // Section 2: create consumer configs
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // TODO: Section 2.0 : Key and Value deserializer class

        // TODO: Section 2.1 : Configurations for SR

        // TODO: Section 3: create consumer

        // TODO: Section 4: subscribe consumer to our topic(s)

        // Section 5: poll for new data
        try {
            while (true) {
                // TODO: Section 5.1

            }
        }
        finally{
            // TODO: Section 6: close consumer

        }


    }
}
