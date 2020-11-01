package com.cloudera.kafka.consumer;

// Section: Imports
import com.cloudera.kafka.util.ConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SchemaConsumerV1 {

    public static final Logger logger = LoggerFactory.getLogger(SchemaConsumerV1.class.getName());
    public static void main(String[] args)  throws Exception {
        if(args.length<1){
            System.out.println("Configuration File Required.");
            System.exit(-1);
        }
        String propertiesFile = args[0];

        // Section 1: Get the configs from the properties file
        final ConfigUtil configUtil = new ConfigUtil(propertiesFile);
        // Get arguments
        final String bootstrapServers = configUtil.getProperties("bootstrap-server");
        final String topic = configUtil.getProperties("topic");
        final String groupId = configUtil.getProperties("group");

        // TODO: Section 1.1 : Schema Registry Configs

        // TODO: Section 1.11 : Security Configs

        // Section 2: create consumer configs
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // TODO: Section 2.0 : Key and Value deserializer class

        // TODO: Section 2.1 : Configurations for SR

        // TODO: Section 2.2 : Get a specific version of Schema from SR

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
