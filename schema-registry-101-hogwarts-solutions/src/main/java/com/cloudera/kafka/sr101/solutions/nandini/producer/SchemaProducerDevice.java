package com.cloudera.kafka.sr101.solutions.nandini.producer;

// Section: Imports

import com.cloudera.kafka.util.ConfigUtil;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerde;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL;

public class SchemaProducerDevice {

    public static final Logger logger = LoggerFactory.getLogger(SchemaProducerDevice.class.getName());
    public static void main(String[] args) throws Exception {
        /*if(args.length<1){
            System.out.println("Configuration File Required.");
            System.exit(-1);
        }
        String propertiesFile = args[0];
        */
        String propertiesFile = "/Users/nandinin/Desktop/git/schema-registry-101-hogwarts/resources/configs/producer_device.properties";

        // Section 1: Get the configs from the properties file
        final ConfigUtil configUtil = new ConfigUtil(propertiesFile);

        // Get arguments
        final String bootstrapServers = configUtil.getProperties("bootstrap-server");
        final String topic = configUtil.getProperties("topic");

        // TODO: Section 1.1 : Schema Registry Configs (Schema Registry URL and Schema Name)
        final String SCHEMA_REGISTRY_URL = configUtil.getProperties("schema-registry-url");
        final String SCHEMA_NAME = configUtil.getProperties("schema-name");
        // TODO: Section 1.Secure : Security Configs

        // Section 2: create Producer props
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // TODO: Section 2.0 : Key and Value serializer class
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION, METADATA_ID_VERSION_PROTOCOL);
        // TODO: Section 2.1 : Configurations for SR
        props.setProperty(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);
        // TODO: Section 3: Create a SR Client
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000);
        config.put(KafkaAvroSerializer.STORE_SCHEMA_VERSION_ID_IN_HEADER, "true");
        config.put(KafkaAvroSerde.KEY_SCHEMA_VERSION_ID_HEADER_NAME, "key.schema.version.id");
        config.put(KafkaAvroSerde.VALUE_SCHEMA_VERSION_ID_HEADER_NAME, "device");
        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(config);
        // TODO: Section 3.Secure : Security Configs
        // TODO: Section 3.Secure.SSL
        // TODO: Section 3.Secure.SASL
        // TODO: Section 3.Secure.Knox


        final String device_schema = "{"
                + "\"type\":\"record\","
                + "\"name\":\"device\","
                + "\"fields\":["
                + "  { \"name\":\"dev1\", \"type\":\"string\" },"
                + "  { \"name\":\"dev2\", \"type\":\"string\" },"
                + "  { \"name\":\"dev3\", \"type\":\"string\" }"
                + "]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(device_schema);
        // TODO: Section 4: create the producer
        KafkaProducer<Long, Object> producer = new KafkaProducer<Long, Object>(props);
        // Section 5: create a producer record and send data asynchronously at an interval of 1 sec
        try{

            while(true){
                // TODO: Section 5.1 create a Generic record
                Long timestamp = System.currentTimeMillis();
                GenericRecord avroRecord = new GenericData.Record(schema);
                avroRecord.put("dev1", "dev1");
                avroRecord.put("dev2", "dev2");
                avroRecord.put("dev3", "dev3");
                // TODO: Section 5.2 create a producer record
                ProducerRecord<Long, Object > record = new ProducerRecord<Long, Object >(topic, timestamp, avroRecord);
                // TODO: Section 5.3 sends data asynchronously
               producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            System.out.println("Received new metadata." +
                                    "Topic:" + recordMetadata.topic()  +
                                    ", Partition: " + recordMetadata.partition()  +
                                    ", Offset: " + recordMetadata.offset() +
                                    ", Timestamp: " + recordMetadata.timestamp() +
                                    ", Record: " + record.value());
                        } else {
                            System.err.println("Error while producing" + e);
                        }
                    }
                });
                //sleep for 1 sec
                Thread.sleep(1000);
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            // TODO: Section 6 flushes and close producer
            producer.flush();
            producer.close();
        }
    }
}
