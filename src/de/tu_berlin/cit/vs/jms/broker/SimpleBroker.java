package de.tu_berlin.cit.vs.jms.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

import de.tu_berlin.cit.vs.jms.client.JmsBrokerClient;
import de.tu_berlin.cit.vs.jms.common.BrokerMessage;
import de.tu_berlin.cit.vs.jms.common.BuyMessage;
import de.tu_berlin.cit.vs.jms.common.ListMessage;
import de.tu_berlin.cit.vs.jms.common.RegisterMessage;
import de.tu_berlin.cit.vs.jms.common.SellMessage;
import de.tu_berlin.cit.vs.jms.common.Stock;
import de.tu_berlin.cit.vs.jms.common.UnregisterMessage;


public class SimpleBroker {
    /* TODO: variables as needed */
	static int nextId = 0;
	
	List<Stock> stocks = new ArrayList<>();
	List<JmsBrokerClient> clients;
	
	
	MessageProducer producer;
	MessageConsumer consumer;
	Session session;
	
    private final MessageListener listener = new MessageListener() {
        @Override
        public void onMessage(Message msg) {
            if(msg instanceof ObjectMessage) {
                //TODO
            	
            	try {
            		BrokerMessage brokMsg = (BrokerMessage)((ObjectMessage) msg).getObject();
					
            		switch(brokMsg.getType()) {
						case STOCK_BUY:
							BuyMessage buyMsg = (BuyMessage)((ObjectMessage) msg).getObject();
							int stockIndex = buy(buyMsg.getStockName(), buyMsg.getAmount());
							Stock targetStock = stocks.get(stockIndex);
							targetStock.setAvailableCount(targetStock.getAvailableCount() - 1);
					    	targetStock.setStockCount(targetStock.getStockCount() +  1);
							break;
						case STOCK_SELL:
							SellMessage sellMsg = (SellMessage)((ObjectMessage) msg).getObject();
							sell(sellMsg.getStockName(), sellMsg.getAmount());
							break;
						case STOCK_LIST:
							ObjectMessage listMsg = session.createObjectMessage(new ListMessage(stocks));
							producer.send(listMsg);
							break;
						case SYSTEM_REGISTER:
							RegisterMessage regMsg = (RegisterMessage)((ObjectMessage) msg).getObject();
							Queue in = session.createQueue("incoming");
							Queue out = session.createQueue("outcoming");
							clients.add(new JmsBrokerClient(nextId++, regMsg.getClientName(), in, out));
							break;
						case SYSTEM_UNREGISTER: 
							UnregisterMessage unregMsg = (UnregisterMessage)((ObjectMessage) msg).getObject();
							if (clients.stream().
									filter( c -> c.getClientName().equals(unregMsg.getClientName()) ).count() == 0 ) {
								System.out.println("Client is not registered. Please register in prior.");
								break;
							}
							
							for (JmsBrokerClient client: clients) {
								if (client.getClientName().equals(unregMsg.getClientName())) {
									clients.remove(client);
								}
							}
							break;
						case SYSTEM_ERROR:
							System.out.println("Error message was detected");
							break;
						default:
							System.out.println("Not a supported command");
							break;
					}
					
				} catch (JMSException e) {
					System.out.println("error message: " + e.getMessage());
					e.printStackTrace();
				}
            	
            }
        }
    };
    
    public SimpleBroker(List<Stock> stockList) throws JMSException {
        /* TODO: initialize connection, sessions, etc. */
        SQSConnectionFactory conFactory = new SQSConnectionFactory(
        		new ProviderConfiguration(), 
        		AmazonSQSClientBuilder.standard().withRegion("us-east-2"));
        SQSConnection con = conFactory.createConnection(
        		"AKIAIBTHVB24KIISRKRQ", 
        		"g3/ks/Y8SwjnztgAVDPy0PmXiXPUk/fvEeOwnCIS");
        con.start();
        
        this.session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue myqueue = session.createQueue("myQueue");
        
        this.consumer = session.createConsumer(myqueue);
        this.producer = session.createProducer(myqueue);
    	
    	
        for(Stock stock : stockList) {
            /* TODO: prepare stocks as topics */
        	Topic topic = session.createTopic(stock.getName());
        }
        
        this.stocks = stockList;
        session.setMessageListener(listener);
        // registration Queue
     // Get the wrapped client
        AmazonSQSMessagingClientWrapper client = con.getWrappedAmazonSQSClient();

        // Create an Amazon SQS FIFO queue named MyQueue.fifo, if it doesn't already exist
        if (!client.queueExists("RegQueue.fifo")) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put("FifoQueue", "true");
            attributes.put("ContentBasedDeduplication", "true");
            client.createQueue(new CreateQueueRequest().withQueueName("RegQueue.fifo").withAttributes(attributes));
        }
    }
    
    public void stop() throws JMSException {
        //TODO
    	System.out.println("request from broker server to stop the simple broker");
    	System.exit(2);
    }
    
    public synchronized int buy(String stockName, int amount) throws JMSException {
        //TODO
    	for (int i = 0; i < this.stocks.size(); i++) {
    		if (stocks.get(i).getName().equals(stockName)) {
    			return i;
    		}
    	}
        return -1;
    }
    
    public synchronized int sell(String stockName, int amount) throws JMSException {
        //TODO
    	
        return -1;
    }
    
    public synchronized List<Stock> getStockList() {
        List<Stock> stockList = new ArrayList<>();

        /* TODO: populate stockList */
        stockList.addAll(this.stocks);

        return stockList;
    }
}
