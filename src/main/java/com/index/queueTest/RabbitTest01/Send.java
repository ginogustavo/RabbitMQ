package com.index.queueTest.RabbitTest01;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.util.Date;

import com.rabbitmq.client.Channel;

public class Send {

	private final static String QUEUE_NAME = "hello";

	public static void main(String[] args) throws Exception {

		//connection to the server
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		//Channel
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		String message = "Message for " + new Date();
		channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
		System.out.println("[x] Sent '" + message + "'");

		channel.close();
		connection.close();

	}

	
}
