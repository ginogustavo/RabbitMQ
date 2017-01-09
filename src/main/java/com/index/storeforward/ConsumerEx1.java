package com.index.storeforward;


import java.io.UnsupportedEncodingException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

public class ConsumerEx1 {

	public static void main(String[] args) throws Exception{
		ConnectionFactory factory = new ConnectionFactory();

		// set connection info
		factory.setHost("localhost");
		factory.setUsername("guest");
		factory.setPassword("guest");

		// create the connection
		Connection connection = factory.newConnection();

		// create the channel
		Channel channel = connection.createChannel();
		
		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume("sfQueue", consumer);
		
		boolean removeAllUpTo = true;
		while(true){
			Delivery delivery = consumer.nextDelivery(5000);
			if ( delivery == null) break;
			if ( processMessage(delivery) ){
				long deliveryTag = delivery.getEnvelope().getDeliveryTag();
				channel.basicAck(deliveryTag, removeAllUpTo);
			}
		}

		channel.close();
		connection.close();

	}

	private static boolean processMessage(Delivery delivery) throws UnsupportedEncodingException {
		String msn = new String(delivery.getBody(), "UTF-8");
		System.out.println(" [x] Recv : redeliver = " + delivery.getEnvelope().isRedeliver() + ", msg = Hello World! msg # " +msn);
		return true;
	}

}
