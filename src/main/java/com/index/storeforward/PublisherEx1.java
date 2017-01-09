package com.index.storeforward;

import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;


/**
 * Publish messages to the message queue
 * @author Gustavo
 *
 */
public class PublisherEx1 {

	public static void main(String[] args) throws IOException, TimeoutException {

		ConnectionFactory factory = new ConnectionFactory();
		
		//set connection info
		factory.setHost("localhost");
		factory.setUsername("guest");
		factory.setPassword("guest");
		
		//create the connection
		Connection connection = factory.newConnection();
		
		//create the channel
		Channel channel = connection.createChannel();
		//You can have multiple channel associated from one connection

		//Publish a message (to the queue)
		String queueName = "sfQueue";
		for (int i = 0; i < 10; i++) {
			String message = "Hello World! This is message # " + i;
			//Using that channel you can call basic publish method
			channel.basicPublish("", queueName, null, message.getBytes());
			System.out.println(" [x] Sent '"+message+"'");
		}
		
		//channel.basicPublish()
		//Exchange, RoutingKey(=Name of the queue), the headers, the message as bytes
		
		channel.close();
		connection.close();
	}

}
