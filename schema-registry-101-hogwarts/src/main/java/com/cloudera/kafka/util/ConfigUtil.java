package com.cloudera.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class ConfigUtil {
    String propertiesFile;
    public ConfigUtil(String propertyFilePath ){
        propertiesFile = propertyFilePath;
    }

    final Logger logger = LoggerFactory.getLogger(ConfigUtil.class.getName());
    public String getProperties(String property) throws IOException {
        String result = "";
        InputStream inputStream = null;
        try {
            Properties prop = new Properties();
            inputStream =  new BufferedInputStream(new FileInputStream(propertiesFile));
                    //getClass().getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propertiesFile + "' not found.");
            }

            // get the property value and print it out
            result = prop.getProperty(property).toString();
            if(result.isEmpty()){
                logger.error("Please enter the value for the property " + property );
                System.exit(-1);
            }
            logger.info("Using " + property + " = " + result);
        } catch (Exception e) {
            logger.error("Exception: " + e);
        } finally {
            inputStream.close();
        }

        return result;
    }
}
