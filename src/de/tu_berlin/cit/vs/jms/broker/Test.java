package de.tu_berlin.cit.vs.jms.broker;

import javax.jms.JMSException;
import javax.jms.Session;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import javax.jms.*;

public class Test {

	public static void main(String[] args) throws JMSException {
		
		SQSConnectionFactory conFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.standard().withRegion("us-east-2")
        );


        SQSConnection connection = conFactory.createConnection(
                "AKIAIBTHVB24KIISRKRQ",
                "g3/ks/Y8SwjnztgAVDPy0PmXiXPUk/fvEeOwnCIS"
        );
        
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue regQueue = session.createQueue("RegistrationQueue");
        MessageProducer producer = session.createProducer(regQueue);
        TextMessage msg = session.createTextMessage("hey");
        producer.send(msg);
        
        
	}
}
