package com.kafka.cps;

import com.kafka.cps.notificationConsumerGroup;

public class kafkaToCpsMain {

	public static void main(String[] args) {

		String brokers = "10.132.15.197:9092";
		String groupId = "test-consumer-group1";
		String topic = "to-pubsub-nifi-11";
		String CPStopic = "from-kafka1";
		int numberOfConsumer = 1;

		if (args != null && args.length >= 4) {
			brokers = args[0];
			groupId = args[1];
			topic = args[2];
			CPStopic = args[3];
			numberOfConsumer = 1;
		}
		// Start group of Notification Consumers
		notificationConsumerGroup consumerGroup = new notificationConsumerGroup(brokers, groupId, topic,
				numberOfConsumer, CPStopic);

		consumerGroup.execute();

		try {
			Thread.sleep(100000);
		} catch (InterruptedException ie) {

		}
	}

}
