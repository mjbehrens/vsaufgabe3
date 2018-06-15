package de.tu_berlin.cit.vs.jms.client;


import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import javax.jms.*;


public class AWSSQSClient {



    public static void main(String[] args) {
        // Create a new connection factory with all defaults (credentials and region) set automatically
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.standard()
                        .withRegion("us-east-2")
        );

        // Create the connection.
        try {
            //String awsAccessKeyId ="AKIAJLU2JOEMAHIDPMGQ";
            //String awsSecretKey = "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";
            SQSConnection connection = connectionFactory.createConnection();


            // Get the wrapped client
            AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();


            if (!client.queueExists("MyQueue")) {
                client.createQueue("MyQueue");
            }

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a queue identity and specify the queue name to the session
            Queue queue = session.createQueue("newQueue");

            // Create a producer for the 'MyQueue'
            MessageProducer producer = session.createProducer(queue);

            System.out.println("test");

            // Create the text message
            TextMessage message = session.createTextMessage("Hello World!");

            // Send the message
            producer.send(message);
            System.out.println("JMS Message " + message.getJMSMessageID());


        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

}
