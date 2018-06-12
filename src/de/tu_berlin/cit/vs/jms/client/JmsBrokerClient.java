package de.tu_berlin.cit.vs.jms.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import de.tu_berlin.cit.vs.jms.common.BuyMessage;
import de.tu_berlin.cit.vs.jms.common.RegisterMessage;
import de.tu_berlin.cit.vs.jms.common.RequestListMessage;
import de.tu_berlin.cit.vs.jms.common.SellMessage;
import de.tu_berlin.cit.vs.jms.common.UnregisterMessage;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;


public class JmsBrokerClient {
    
	private Queue in;
	private Queue out;
	private String clientName;
	private int id;

	MessageConsumer consumer;
	MessageProducer producer;
	Session session;
	
	public JmsBrokerClient(int id, String clientName, Queue in, Queue out) {
		this.id = id;
		this.clientName = clientName;
		this.in = in;
		this.out = out;
	}
	
	public JmsBrokerClient(String clientName) throws JMSException {
        this.clientName = clientName;
        
        /* TODO: initialize connection, sessions, consumer, producer, etc. */
        SQSConnectionFactory conFactory = new SQSConnectionFactory(
        		new ProviderConfiguration(), 
        		AmazonSQSClientBuilder.standard().withRegion("us-east-2")
        );
        SQSConnection connection = conFactory.createConnection(
        		"AKIAIBTHVB24KIISRKRQ", 
        		"g3/ks/Y8SwjnztgAVDPy0PmXiXPUk/fvEeOwnCIS"
        );
        connection.start();
        
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // Connect to registration queue
        Queue regQueue = session.createQueue("RegistrationQueue");
        this.producer = session.createProducer(regQueue);
    }
	
	public Queue getIn() {
		return in;
	}

	public Queue getOut() {
		return out;
	}

	public String getClientName() {
		return clientName;
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
		
	public void register() throws JMSException {
        ObjectMessage regMsg = this.session.createObjectMessage(new RegisterMessage(this.clientName));
        this.producer.send(regMsg);
        System.out.println("Register msg was sent to the broker");
	}
	
	public void unregister() throws JMSException {
		ObjectMessage unregMsg = session.createObjectMessage(new UnregisterMessage(this.clientName));
    	this.producer.send(unregMsg);
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
    }
    
    public void unwatch(String stockName) throws JMSException {
        //TODO
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
                        case "register":
                        	if(task.length == 1) {
                        		client.register();
                        	} else {
                        		System.out.println("Correct usage: register");
                        	}
                        	break;
                        case "unregister":
                        	if(task.length == 1) {
                        		client.unregister();
                        	} else {
                        		System.out.println("Correct usage: unregister");
                        	}
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
