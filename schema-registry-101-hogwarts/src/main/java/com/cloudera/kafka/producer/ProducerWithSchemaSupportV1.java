package com.cloudera.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithSchemaSupportV1 {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerWithSchemaSupportV1.class.getName());

        // Section 1: command line arguments
        // Section 1.0 : Client Mandatory configs
        final String bootstrapServers = args[0]; //e.g: "127.0.0.1:9092"
        final String topic = args[1]; //e.g: "test_topic"
        // TODO: Section 1.1 : Schema Registry Configs

        // TODO: Section 1.2 : Schema Name

        // TODO: Section 1.11 : Security Configs

        // Section 2: create Producer props
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // TODO: Section 2.0 : Key and Value serializer class

        // TODO: Section 2.1 : Configurations for SR

        // TODO: Section 3: Create a SR Client

        // TODO: Section 3.1: Get latest version of Schema

        // TODO: Section 3.2: Create schema

        // TODO: Section 4: create the producer

        // Section 5: create a producer record and send data asynchronously at an interval of 1 sec
        try{

            while(true){
                // TODO: Section 5.1 create a Generic record

                // TODO: Section 5.2 create a producer record

                // TODO: Section 5.3 sends data asynchronously

                //sleep for 1 sec
                Thread.sleep(1000);
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            // TODO: Section 6 flushes and close producer

        }
    }
}
