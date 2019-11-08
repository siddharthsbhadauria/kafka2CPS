package com.kafka.cps;

import java.util.ArrayList;
import java.util.List;

public final class notificationConsumerGroup {
	private final int numberOfConsumers;
	private final String groupId;
	private final String topic;
	private final String brokers;
	private final String CPStopic;
	private List<notificationConsumerThread> consumers;

	public notificationConsumerGroup(String brokers, String groupId, String topic, int numberOfConsumers,
			String CPStopic) {
		this.brokers = brokers;
		this.topic = topic;
		this.groupId = groupId;
		this.numberOfConsumers = numberOfConsumers;
		this.CPStopic = CPStopic;
		consumers = new ArrayList<>();
		for (int i = 0; i < this.numberOfConsumers; i++) {
			notificationConsumerThread ncThread = new notificationConsumerThread(this.brokers, this.groupId, this.topic,
					this.CPStopic);
			consumers.add(ncThread);
		}
	}

	public void execute() {
		for (notificationConsumerThread ncThread : consumers) {
			Thread t = new Thread(ncThread);
			t.start();
		}
	}

	/**
	 * @return the numberOfConsumers
	 */
	public int getNumberOfConsumers() {
		return numberOfConsumers;
	}

	/**
	 * @return the groupId
	 */
	public String getGroupId() {
		return groupId;
	}

}
