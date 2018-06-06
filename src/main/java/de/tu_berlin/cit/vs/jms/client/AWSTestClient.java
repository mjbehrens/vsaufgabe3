package de.tu_berlin.cit.vs.jms.client;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazon.sqs.javamessaging.P
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public class AWSTestClient {


    public static void main(String[] args) {

        // Create a new connection factory with all defaults (credentials and region) set automatically
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.defaultClient()
        );

        // Create the connection.
        SQSConnection connection = connectionFactory.createConnection();


        // Get the wrapped client
        AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();

// Create an SQS queue named MyQueue, if it doesn't already exist
        if (!client.queueExists("MyQueue")) {
            client.createQueue("MyQueue");
        }


    }
}
