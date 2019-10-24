package com.example.demo;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.util.concurrent.ListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

@SpringBootApplication
public class SpringbootKafkaApplication {

	private final Logger logger = LoggerFactory.getLogger(SpringbootKafkaApplication.class);

	@Autowired
	private KafkaTemplate<Object, Object> template;

	@Autowired
	private ObjectMapper objectMapper;

	public static void main(String[] args) {
		SpringApplication.run(SpringbootKafkaApplication.class, args);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory, KafkaTemplate<Object, Object> template) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setErrorHandler(
				new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(template), new FixedBackOff(0L, 2))); // dead-letter
																														// after
																														// 3
																														// tries
		return factory;
	}

	@Bean
	public RecordMessageConverter converter() {
		return new StringJsonMessageConverter();
	}

	@KafkaListener(id = "fooGroup", topics = "topic1")
	public void listen(Foo1 foo) {
		logger.info(">> Received: " + foo);
		if (foo.getFoo().startsWith("deadLetterTopic")) {
			throw new RuntimeException("failed");
		}
		if (foo.getFoo().startsWith("requeue")) {
			throw new RuntimeException("failed");
		}
	}

	@KafkaListener(id = "dltGroup", topics = "topic1.DLT")
	public void dltListen(String in)
			throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		logger.info(">> Received from DLT: " + in);

		final JsonNode jsonNode = objectMapper.readTree(in);

		final Foo1 foo1 = Optional.ofNullable(jsonNode).map(value -> objectMapper.convertValue(value, Foo1.class))
				.orElse(null);

		System.out.println("foo1 >> " + foo1.getFoo());

		if (foo1.getFoo().startsWith("requeue")) {

			final ListenableFuture<SendResult<Object, Object>> response = this.template.send("topic1",
					new Foo1(UUID.randomUUID().toString() + "_" + foo1.getFoo()));
			System.out.println("response.isDone() : " + response.isDone());
			System.out.println("response.isDone() : " + response.get().getProducerRecord().value().toString());
		}
	}

	@Bean
	public NewTopic topic() {
		logger.info("topic1 created");
		return new NewTopic("topic1", 1, (short) 1);
	}

	@Bean
	public NewTopic dlt() {
		logger.info(">> topic1.DLT created");
		return new NewTopic("topic1.DLT", 1, (short) 1);
	}

	@Bean
	public ObjectMapper getObjectMapper() {
		final ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		final SimpleModule module = new SimpleModule();
		module.addSerializer(BigDecimal.class, new ToStringSerializer());
		mapper.registerModule(module);

		return mapper;
	}
}
