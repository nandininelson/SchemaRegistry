package com.cloudera.kafka.producer;

// Section: Imports
import com.cloudera.kafka.util.ConfigUtil;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Properties;

public class SchemaProducerV1 {

    public static final Logger logger = LoggerFactory.getLogger(SchemaProducerV1.class.getName());
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

        // TODO: Section 1.1 : Schema Registry Configs (Schema Registry URL and Schema Name)

        // TODO: Section 1.Secure : Security Configs

        // Section 2: create Producer props
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // TODO: Section 2.0 : Key and Value serializer class

        // TODO: Section 2.1 : Configurations for SR

        // TODO: Section 3: Create a SR Client

        // TODO: Section 3.Secure : Security Configs
        // TODO: Section 3.Secure.SSL
        // TODO: Section 3.Secure.SASL
        // TODO: Section 3.Secure.Knox


        // TODO: Section 3.1: Get a specific version of Schema from SR

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
