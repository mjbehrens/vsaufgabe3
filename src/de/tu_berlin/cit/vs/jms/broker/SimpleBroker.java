package de.tu_berlin.cit.vs.jms.broker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.*;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
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
	List<JmsBrokerClient> clients = new ArrayList<>();
	// if client wants to sell stocks -> check whether he owns the stock
	Map<String, List<Stock>> userStocksMap = new HashMap<>();
	
	
	
	MessageProducer producer;
	MessageConsumer consumer;
	Session session;
	SQSConnection con;
	
    private final MessageListener listener = new MessageListener() {
        @Override
        public void onMessage(Message msg) {
            if(msg instanceof ObjectMessage) {
                //TODO
            	try {
            		
            		BrokerMessage brokMsg = (BrokerMessage)((ObjectMessage) msg).getObject();
            		System.out.println("Broker: msg type = " + brokMsg.getType());
					
            		switch(brokMsg.getType()) {
						case STOCK_BUY:
							BuyMessage buyMsg = (BuyMessage)((ObjectMessage) msg).getObject();
							System.out.println("name: " + msg.getStringProperty("name"));
							buy(buyMsg.getStockName(), buyMsg.getAmount(), msg.getStringProperty("name"));
					    	break;
						case STOCK_SELL:
							/*SellMessage sellMsg = (SellMessage)((ObjectMessage) msg).getObject();
							if (
									clientGotStocks(
											sellMsg.getStockName(),
											msg.getStringProperty("name"),
											sellMsg.getAmount())
							) {
								sell(sellMsg.getStockName(), sellMsg.getAmount(),msg.getStringProperty("name"));
							}*/
							break;
						case STOCK_LIST:
							ObjectMessage listMsg = session.createObjectMessage(new ListMessage(stocks));
							System.out.println("Broker: sending the stock list");
							producer.send(listMsg);
							break;
						case SYSTEM_REGISTER:
							RegisterMessage regMsg = (RegisterMessage)((ObjectMessage) msg).getObject();
							Queue in = session.createQueue("newQueue");
							Queue out = session.createQueue("RegistrationQueue");
							userStocksMap.put(
									regMsg.getClientName(), 
									new ArrayList<Stock>()
							);
							clients.add(new JmsBrokerClient(nextId++, regMsg.getClientName(), in, out));
							sendMessage(
									regMsg.getClientName(),
									"client has registered successfully"
							);
							break;
						case SYSTEM_UNREGISTER: 
							UnregisterMessage unregMsg = (UnregisterMessage)((ObjectMessage) msg).getObject();
							if (getClientByName(unregMsg.getClientName()) == null) {
								System.out.println("Client is not registered. Please register in prior.");
								sendMessage(
										unregMsg.getClientName(), 
										"Client is not registered. Please register in prior."
								);
								break;
							}
							
							Iterator<JmsBrokerClient> it = clients.iterator();
							while (it.hasNext()) {
								JmsBrokerClient cl = it.next();
								if (cl.getClientName().equals(unregMsg.getClientName())) {
									it.remove();
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
    	System.out.println("SimpleBroker");
    	SQSConnectionFactory conFactory = new SQSConnectionFactory(
        		new ProviderConfiguration(), 
        		AmazonSQSClientBuilder.standard().withRegion("us-east-2")
        );
    	
    	
        this.con = conFactory.createConnection(
        		"AKIAIBTHVB24KIISRKRQ", 
        		"g3/ks/Y8SwjnztgAVDPy0PmXiXPUk/fvEeOwnCIS"
        );
        
        AmazonSQSMessagingClientWrapper client = con.getWrappedAmazonSQSClient();
        
        if (!client.queueExists("RegistrationQueue")) {
        	client.createQueue("RegistrationQueue");
        }
        
        this.session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // queue for polling registration requests
        Queue regQueue = session.createQueue("RegistrationQueue");
        this.consumer = session.createConsumer(regQueue);
        
        Queue newQueue = session.createQueue("newQueue");
        this.producer = session.createProducer(newQueue);
    	
        consumer.setMessageListener(this.listener);
        con.start();
//        for(Stock stock : stockList) {
//            /* TODO: prepare stocks as topics */
//        	Topic topic = session.createTopic(stock.getName());
//        }
        
        this.stocks = stockList;
    }
    


	public void stop() throws JMSException {
        //TODO
    	System.out.println("request from broker server to stop the simple broker");
    	this.con.close();
    	System.exit(2);
    }
    
    public synchronized void buy(String stockName, int amount, String clientName) throws JMSException {
    	// client exists (is registered)
    	if (getClientByName(clientName) == null) {
    		sendMessage(
    				clientName, 
    				"cannot invoke buy or sell. Please register in prior!"
    		);
    		return;
    	}
    	Stock targetStock = getStockByName(stockName, stocks);
    	// stock exists (valid name)
    	if (targetStock != null) {
    		// requested stock number is available
    		if (targetStock.getAvailableCount() > amount) {
        		targetStock.setAvailableCount(targetStock.getAvailableCount() - amount);
        		// replace old stock obj with updated stock obj
        		for (int i = 0; i < this.stocks.size(); i++) {
        			if (this.stocks.get(i).getName().equals(stockName)) {
        				stocks.set(i, targetStock);
        			}
        		}
        		// user owns the stock and is only updated
        		List<Stock> clientStocks = userStocksMap.get(clientName);
        		int i = 0;
        		if (getStockByName(stockName, userStocksMap.get(clientName)) != null) {
        			for (; i < clientStocks.size(); i++) {
        				if (clientStocks.get(i).getName().equals(stockName)) {
        					
        				}
        			}
        			clientStocks.set(i, targetStock);
        			userStocksMap.put(clientName, clientStocks);
        			
        		} else {
        		// user does not own the stock and has to be added to the map	
        			userStocksMap.put(
        					clientName, 
        					Stream.concat(userStocksMap.get(clientName).stream(), Stream.of(targetStock)).collect(Collectors.toList())
        			);
        		}
        		
        	} else {
        		sendMessage(clientName, "number of requested stock");
        	}
    	} else {
    		sendMessage(clientName, "requested stock does not exist");
    	}
    }
    
    public JmsBrokerClient getClientByName(String clientName) {
    	return clients.stream().filter(c -> c.getClientName().equals(clientName)).findFirst().orElse(null);
    }
    
    public Stock getStockByName(String stockName, List<Stock> stocks) {
    	return stocks.stream().filter(s -> s.getName().equals(stockName)).findFirst().orElse(null);
    }
    
    public void sendMessage(String clientName, String content) throws JMSException {
    	TextMessage msg = session.createTextMessage(content);
		JmsBrokerClient cl = getClientByName(clientName);
		if (cl != null) {
			this.producer = session.createProducer(getClientByName(clientName).getIn());
			this.producer.send(cl.getIn(), msg);
		} else {
			TextMessage regMsg = session.createTextMessage(content);
			this.producer.send(regMsg);
		}
    }
    
    
   /*protected void clientAddStocks(String stockName, String clientName, int amount) {
		// TODO Auto-generated method stub
    	JmsBrokerClient Client = getClientByName(clientName);
    	Stock stock = getStockByName(stockName,Client.getStocks());
    	if(stock == null)
    	{
    		
    	}
		
	}
	private boolean clientGotStocks(String stockName, String clientName, int amount ) {
		JmsBrokerClient Client = getClientByName(clientName);
		Stock stock = getStockByName(stockName,Client.getStocks());
		if (stock==null || stock.getAvailableCount()< amount )
		{
			return false;
		}
		else
		{
			return true;
		}
	}*/
	
	
    public synchronized boolean sell(String stockName, int amount, String clientName) throws JMSException {
        //TODO
        for (int i = 0; i < this.stocks.size(); i++)
        {
            if (stocks.get(i).getName().equals(stockName)) 
            {
            	
                if(stocks.get(i).getAvailableCount()+amount > stocks.get(i).getStockCount())
                {
                    System.out.println("sell impossible, total amount of stocks exceeded the initial stock count");
                    sendMessage(clientName, "sell impossible, total amount of stocks exceeded the initial stock count");
                    return false;
                }
                else
                {
                    stocks.get(i).setAvailableCount(stocks.get(i).getAvailableCount()+amount);
                    return true;
                    
                }   
            }    
        }
        // if it doesn't find the stock in the stocks list
        sendMessage(clientName, "requested stock does not exist");
        return false;
    }
    
    public synchronized List<Stock> getStockList() {
        List<Stock> stockList = new ArrayList<>();

        /* TODO: populate stockList */
        stockList.addAll(this.stocks);

        return stockList;
    }
}
