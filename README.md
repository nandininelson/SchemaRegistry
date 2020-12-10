# SchemaRegistry
1. Make sure you have Java(1.8) and Maven(3.3+) installed in your system for the exercise. 
    Hint: run the below commands on terminal to verify the versions
$ java -version
$ mvn --version

2. Go to the downloaded project.
$ cd schema-registry-101-hogwarts

3. Open the file schema-registry-101-hogwarts/pom.xml in a text editor/IDE.
View the  Section A. We have added dependency for kafka-clients under the dependency to use Kafka Client APIs.
View the Section D. We have added plugin for maven-shade-plugin to create 1 jar with all the dependent libraries and given it a name.

4. Run the below command to create an executable jar file under the schema-registry-101-hogwarts/target folder.
$ mvn package 
$ ls target/schema-registry-101-hogwarts.jar 

5. Code Overview:
a. Producer: Open the file schema-registry-101-hogwarts/src/main/java/com/cloudera/kafka/producer/SimpleProducer in a text editor/IDE.
View the Section 1. We are reading the properties defined in the properties file(which will be passed as an argument).
View the Section 2. We are creating the Properties object for the Producer object.
View the Section 3. We create the Kafka Producer by calling the Kafka Client APIs.
View the Section 4.We send the data records to a Kafka Topic.

b. Consumer: Open the file schema-registry-101-hogwarts/src/main/java/com/cloudera/kafka/consumer/SimpleConsumer in a text  editor/IDE.
View the Section 1. We are reading the properties defined in the properties file(which will be passed as an argument).
View the Section 2. We are creating the Properties object for the Consumer object.
View the Section 3. We create the Kafka Consumer by calling the Kafka Client APIs.
View the Section 4. We subscribe to a Kafka Topic.
View the Section 5. We receive and print the data records.

6. Just as we specify the properties for console Kafka Producer and Consumers;
To test this jar created in Step 4, we will create the producer.properties and consumer.properties file using the template files provided under the schema-registry-101-hogwarts/resources folder of the project. Replace the placeholders with correct details.
The Producer requires 2 arguments: bootstrap server and topic name.
The Consumer requires 3 arguments: bootstrap server, topic name and group id. 
$ cat producer.properties 
bootstrap-server=c1528-node2.coelab.cloudera.com:9092,c1528-node3.coelab.cloudera.com:9092,c1528-node4.coelab.cloudera.com:9092
topic=device

$ cat consumer.properties 
bootstrap-server=c1528-node2.coelab.cloudera.com:9092,c1528-node3.coelab.cloudera.com:9092,c1528-node4.coelab.cloudera.com:9092
topic=device
group=device_exercise0


7. Create a topic device on the cluster. We will be using this topic for the exercises ahead.
$ ./kafka-topics.sh --zookeeper localhost:2181 --partitions 3 --replication-factor 3 --create --topic device

8. To run the Kafka Producer use the below command. Keep the producer running until the next step.
$ java -cp schema-registry-101-hogwarts.jar com.cloudera.kafka.producer.SimpleProducer <full-path>/producer.properties

9. To run the Kafka Consumer, open another terminal and use the below command.
$ java -cp schema-registry-101-hogwarts.jar com.cloudera.kafka.consumer.SimpleConsumer <full-path>/consumer.properties

10. Observe the output. You will receive data sent from the Kafka Producer to the Kafka Consumer.
SimpleProducer
SimpleConsumer

Note: The above example does not use schema to produce and consume the data. Now, let’s integrate Schema Registry with the above Kafka Client Applications.
