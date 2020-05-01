package io.aiven.sa;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
	
	private final static String TOPIC = "java-quickstart-kafka-topic";
	private final static String ServiceURI = "kafka-e271828-david-demo.aivencloud.com:24590";
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Logger logger = LoggerFactory.getLogger(Main.class);
		logger.info("This is how you configure Java Logging with SLF4J");

		System.out.println("\nConnecting to cluster@" + ServiceURI + "\n");
		String key = UUID.randomUUID().toString();
		CompletableFuture<Void> consumerFuture = runConsumerExample(key);
		runProducerExample(key);
		
		consumerFuture.get();
		
		System.out.println("\nDone! Cleaning up...");
	}
	
	private static void addTlsProps(Properties props) {		
		props.put("security.protocol", "SSL");
		props.put("ssl.endpoint.identification.algorithm", "");
		props.put("ssl.truststore.location", "client.truststore.jks");
		props.put("ssl.truststore.password", "secret");
		props.put("ssl.keystore.type", "PKCS12");
		props.put("ssl.keystore.location", "client.keystore.p12");
		props.put("ssl.keystore.password", "secret");
		props.put("ssl.key.password", "secret");
	}

	private static void runProducerExample(String targetKey) {
		Properties props = new Properties();
		addTlsProps(props);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceURI);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        
        System.out.println("\nProducer: Sending kafka message\n");
        producer.send(new ProducerRecord<String, String>(TOPIC, targetKey, "Hello Aiven Kafka, from Java!"));
        
        producer.close();
	}
	
	private static CompletableFuture<Void> runConsumerExample(String expectedKey) {
		Properties props = new Properties();
		addTlsProps(props);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceURI);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "java-quickstart-kafka-cgroup");
        
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // start a back ground thread to
        CompletableFuture<Void> future = new CompletableFuture<>();
        new Thread(() -> {
        	consumer.subscribe(Arrays.asList(TOPIC));
        	
        	boolean found = false;
        	while (!found) {
        		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        		for (ConsumerRecord<String, String> record : records) {
        			System.out.printf("Consumed: topic %s partition=%d offset=%d key=%s value=%s\n",
        					record.topic(), record.partition(), record.offset(), record.key(), record.value());
        			found = expectedKey.equals(record.key());
        		}
        	}
        	consumer.close();
        	future.complete(null);
        }).start();
        
        return future;
	}
}
