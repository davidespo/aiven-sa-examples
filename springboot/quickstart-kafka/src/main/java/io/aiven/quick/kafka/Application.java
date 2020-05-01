package io.aiven.quick.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class Application implements CommandLineRunner {

	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Value("${demo.serviceURI}")
	private String serviceURI;
	@Value("${demo.topic}")
	private String topic;

	@Autowired
	private KafkaTemplate<String, String> template;

	private final CountDownLatch latch = new CountDownLatch(1);

	@Override
	public void run(String... args) throws Exception {
		logger.info("Connecting to cluster@{}", serviceURI);
		this.template.send(topic, UUID.randomUUID().toString(), "Hello Aiven Kafka, from SpringBoot!");
		latch.await(60, TimeUnit.SECONDS);
		logger.info("Done! Cleaning up...");
	}

	@KafkaListener(topics = "${demo.topic}")
	public void listen(ConsumerRecord<?, ?> cr) {
		logger.info("Consumed: topic={} partition={} offset={} key={} value={}",
				cr.topic(), cr.partition(), cr.offset(), cr.key(), cr.value());
		latch.countDown();
	}
}
