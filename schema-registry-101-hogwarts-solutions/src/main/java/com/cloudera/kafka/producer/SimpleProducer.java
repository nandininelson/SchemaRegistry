package com.cloudera.kafka.producer;

import com.cloudera.kafka.util.ConfigUtil;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Properties;

public class SimpleProducer {

    public final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class.getName());
    public static void main(String[] args) throws Exception {
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
                            System.out.println("Received new metadata." +
                                    "Topic:" + recordMetadata.topic()  +
                                    ", Partition: " + recordMetadata.partition()  +
                                    ", Offset: " + recordMetadata.offset() +
                                    ", Timestamp: " + recordMetadata.timestamp());
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
            // flush data
            producer.flush();
            // flushes and close producer
            producer.close();
        }
    }

}
