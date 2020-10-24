package com.cloudera.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(SimpleProducer.class.getName());

        // Section 1: command line arguments
        final String bootstrapServers = args[0]; //e.g: "127.0.0.1:9092"
        final String topic = args[1]; //e.g: "test_topic"

        // Section 2: create Producer props
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Section 3: create the producer
        KafkaProducer<Long, String> producer = new KafkaProducer<Long, String>(props);

        // Section 4: create a producer record and send data asynchronously at an interval of 1 sec
        try{

            while(true){

                // create a producer record
                long timestamp = System.currentTimeMillis();
                ProducerRecord record = new ProducerRecord<Long, String >(topic, timestamp, Long.toString(timestamp));

                // sends data asynchronously
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

                //sleep for 1 sec
                Thread.sleep(1000);
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            // flush data
            producer.flush();
            // flushes and close producer
            producer.close();
        }
    }
}
