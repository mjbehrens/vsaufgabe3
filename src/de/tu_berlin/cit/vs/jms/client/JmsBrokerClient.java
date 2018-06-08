package de.tu_berlin.cit.vs.jms.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import de.tu_berlin.cit.vs.jms.common.BuyMessage;
import de.tu_berlin.cit.vs.jms.common.ListMessage;
import de.tu_berlin.cit.vs.jms.common.RegisterMessage;
import de.tu_berlin.cit.vs.jms.common.RequestListMessage;
import de.tu_berlin.cit.vs.jms.common.SellMessage;
import de.tu_berlin.cit.vs.jms.common.Stock;
import de.tu_berlin.cit.vs.jms.common.UnregisterMessage;
import org.apache.activemq.ActiveMQConnectionFactory;


public class JmsBrokerClient {
    
	private String clientName;
	private MessageConsumer consumer;
	private MessageProducer producer;
	private Session session;
	private MessageProducer RegProducer;
	
	public JmsBrokerClient(String clientName) throws JMSException {
        this.clientName = clientName;
        
        /* TODO: initialize connection, sessions, consumer, producer, etc. */
        ActiveMQConnectionFactory conFactory = new ActiveMQConnectionFactory(
        		"tcp://localhost:61616");
        Connection connection = conFactory.createConnection();
        connection.start();
        
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // create an incoming and outcoming queue
        Queue inQueue = session.createQueue("inqueue");
        Queue outQueue = session.createQueue("outqueue");
        
        this.consumer = session.createConsumer(inQueue);
        this.producer = session.createProducer(outQueue);
        // Connect to registreation queue
        Queue queue = session.createQueue("RegQueue");
        this.RegProducer = session.createProducer(queue);

        
        
        
    }
	public void register() throws JMSException {
       
        //message.setStringProperty("JMSXGroupID", "Default");
        ObjectMessage RegMsg = session.createObjectMessage(new RegisterMessage(clientName));
        this.RegProducer.send(RegMsg);
        
	}
    
    public void requestList() throws JMSException {
        //TODO
    	ObjectMessage reqListMsg = session.createObjectMessage(new RequestListMessage());
    	this.producer.send(reqListMsg);
    }
    
    public void buy(String stockName, int amount) throws JMSException {
        //TODO
    	ObjectMessage buyMsg = session.createObjectMessage(new BuyMessage(stockName, amount));
    	this.producer.send(buyMsg);
    }
    
    public void sell(String stockName, int amount) throws JMSException {
        //TODO
    	ObjectMessage sellMsg = session.createObjectMessage(new SellMessage(stockName, amount));
    	this.producer.send(sellMsg);
    }
    
    public void watch(String stockName) throws JMSException {
        //TODO
    	ObjectMessage regMsg = session.createObjectMessage(new RegisterMessage(stockName));
    	this.producer.send(regMsg);
    }
    
    public void unwatch(String stockName) throws JMSException {
        //TODO
    	ObjectMessage unregMsg = session.createObjectMessage(new UnregisterMessage(stockName));
    	this.producer.send(unregMsg);
    }
    
    public void quit() throws JMSException {
        //TODO
    	System.out.println("Client wants to finish the session...");
    	System.exit(1);
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter the client name:");
            String clientName = reader.readLine();
            
            JmsBrokerClient client = new JmsBrokerClient(clientName);
            
            boolean running = true;
            while(running) {
                System.out.println("Enter command:");
                String[] task = reader.readLine().split(" ");
                
                synchronized(client) {
                    switch(task[0].toLowerCase()) {
                        case "quit":
                            client.quit();
                            System.out.println("Bye bye");
                            running = false;
                            break;
                        case "list":
                            client.requestList();
                            break;
                        case "buy":
                            if(task.length == 3) {
                                client.buy(task[1], Integer.parseInt(task[2]));
                            } else {
                                System.out.println("Correct usage: buy [stock] [amount]");
                            }
                            break;
                        case "sell":
                            if(task.length == 3) {
                                client.sell(task[1], Integer.parseInt(task[2]));
                            } else {
                                System.out.println("Correct usage: sell [stock] [amount]");
                            }
                            break;
                        case "watch":
                            if(task.length == 2) {
                                client.watch(task[1]);
                            } else {
                                System.out.println("Correct usage: watch [stock]");
                            }
                            break;
                        case "unwatch":
                            if(task.length == 2) {
                                client.unwatch(task[1]);
                            } else {
                                System.out.println("Correct usage: watch [stock]");
                            }
                            break;
                        default:
                            System.out.println("Unknown command. Try one of:");
                            System.out.println("quit, list, buy, sell, watch, unwatch");
                    }
                }
            }
            
        } catch (JMSException | IOException ex) {
            Logger.getLogger(JmsBrokerClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}
