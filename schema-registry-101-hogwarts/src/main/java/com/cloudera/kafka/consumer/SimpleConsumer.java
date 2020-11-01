package com.cloudera.kafka.consumer;

import com.cloudera.kafka.util.ConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {
    public final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class.getName());
    public static void main(String[] args) throws Exception {
        if(args.length<1){
            System.err.println("Configuration File Required.");
            System.exit(-1);
        }
        String propertiesFile = args[0];
        // Section 1: Get the configs from the properties file
        final ConfigUtil configUtil = new ConfigUtil(propertiesFile);
        // Get arguments
        final String bootstrapServers = configUtil.getProperties("bootstrap-server");
        final String topic = configUtil.getProperties("topic");
        final String groupId = configUtil.getProperties("group");

        // Section 2: create consumer configs
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // Section 3: create consumer
        KafkaConsumer<Long, String> consumer = new KafkaConsumer<Long, String>(props);

        // Section 4: subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        // Section 5: poll for new data
        try {
            while (true) {
                ConsumerRecords<Long, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Long, String> record : records) {
                    System.out.println(
                            "Key: " + record.key() +
                            ", Value: " + record.value() +
                            ", Partition: " + record.partition() +
                            ", Offset:" + record.offset());
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
