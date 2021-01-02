package com.cloudera.kafka.sr101.solutions.nandini.consumer;

// Section: Imports

import com.cloudera.kafka.util.ConfigUtil;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SchemaConsumerUser {

    public static final Logger logger = LoggerFactory.getLogger(SchemaConsumerUser.class.getName());
    public static void main(String[] args)  throws Exception {
        /*if(args.length<1){
            System.out.println("Configuration File Required.");
            System.exit(-1);
        }
        String propertiesFile = args[0];*/
        String propertiesFile = "/Users/nandinin/Desktop/git/schema-registry-101-hogwarts/resources/configs/consumer_user.properties";//args[0];

        // Section 1: Get the configs from the properties file
        final ConfigUtil configUtil = new ConfigUtil(propertiesFile);
        // Get arguments
        final String bootstrapServers = configUtil.getProperties("bootstrap-server");
        final String topic = configUtil.getProperties("topic");
        final String groupId = configUtil.getProperties("group");

        // TODO: Section 1.1 : Schema Registry Configs
        final String SCHEMA_REGISTRY_URL = configUtil.getProperties("schema-registry-url");
        final String SCHEMA_NAME = configUtil.getProperties("schema-name");
        // TODO: Section 1.11 : Security Configs

        // Section 2: create consumer configs
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(KafkaAvroSerde.KEY_SCHEMA_VERSION_ID_HEADER_NAME, "key.schema.version.id");
        props.put(KafkaAvroSerde.VALUE_SCHEMA_VERSION_ID_HEADER_NAME, "user");

        // TODO: Section 2.0 : Key and Value deserializer class
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        // TODO: Section 2.1 : Configurations for SR
        props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);
        // TODO: Section 2.2 : Get a specific version of Schema from SR
        Map<String, Integer> readerVersions = new HashMap<>();
        readerVersions.put(SCHEMA_NAME, 1/*SCHEMA_VERSION*/);
        props.put("schemaregistry.reader.schema.versions", readerVersions);

        // TODO: Section 3: create consumer
        KafkaConsumer<Long, Object> consumer = new KafkaConsumer<Long, Object>(props);
        // TODO: Section 4: subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        // Section 5: poll for new data
        try {
            while (true) {
                // TODO: Section 5.1
                ConsumerRecords<Long, Object> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Long, Object> record : records) {
                    System.out.print(
                            "Key: " + record.key() +
                            ", Value: " + record.value() +
                            ", Partition: " + record.partition() +
                            ", Offset:" + record.offset() +
                            ", Schema Version:" + readerVersions.get(SCHEMA_NAME)
                    );

                    // Section 5.2 verify the record
                    GenericRecord avroRecord = (GenericRecord)record.value();
                    System.out.println(", Value: { xid = " + avroRecord.get("xid") +
                            " name = " + avroRecord.get("name") +
                            " version = " + avroRecord.get("version")+
                            " timestamp = " + avroRecord.get("timestamp") + " }"
                                );
                }
                // commit offsets
                consumer.commitSync();
            }
        }
        finally{
            // TODO: Section 6: close consumer
            consumer.close();
        }


    }
}
