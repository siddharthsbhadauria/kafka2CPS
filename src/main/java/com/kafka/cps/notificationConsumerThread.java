package com.kafka.cps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

public class notificationConsumerThread implements Runnable {

	private final KafkaConsumer<String, String> consumer;
	private final String topic;
	private final String CPStopic;

	public notificationConsumerThread(String brokers, String groupId, String topic, String CPStopic) {
		Properties prop = createConsumerConfig(brokers, groupId);
		this.consumer = new KafkaConsumer<>(prop);
		this.topic = topic;
		this.CPStopic = CPStopic;
		this.consumer.subscribe(Arrays.asList(this.topic));
	}

	private static Properties createConsumerConfig(String brokers, String groupId) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	@Override
	public void run() {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("Receive message: " + record.value() + ", Partition: " + record.partition()
						+ ", Offset: " + record.offset() + ", by ThreadID: " + Thread.currentThread().getId());

				final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
				String topicId = CPStopic;
				ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topicId);
				Publisher publisher = null;
				List<ApiFuture<String>> futures = new ArrayList<>();
				try {
					// Create a publisher instance with default settings bound to the topic
					publisher = Publisher.newBuilder(topicName).build();

					int partition = record.partition();
					long offset = record.offset();
					String message = record.value();

					// convert message to bytes
					ByteString data = ByteString.copyFromUtf8(message);

					PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).putAttributes("Kafka Topic", topic).putAttributes("Partition", Integer.toString(partition)).putAttributes("Offset", Long.toString(offset)).build();							

					// Schedule a message to be published. Messages are automatically batched.
					ApiFuture<String> future = publisher.publish(pubsubMessage);
					System.out.println("Message Published: " + message);
					futures.add(future);
				} catch (IOException e) {
					System.out.println("IO Exception");
					e.printStackTrace();
				} finally {
					// Wait on any pending requests
					List<String> messageIds = new ArrayList<>();
					try {
						messageIds = ApiFutures.allAsList(futures).get();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
					for (String messageId : messageIds) {
						System.out.println(messageId);
					}

					if (publisher != null) {
						// When finished with the publisher, shutdown to free up resources.
						publisher.shutdown();
					}
				}

			}
		}

	}

}
