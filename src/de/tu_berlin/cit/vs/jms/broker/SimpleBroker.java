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
import de.tu_berlin.cit.vs.jms.aws.AWSTopicHandler;
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
    Queue regQueue;

    AWSTopicHandler topicHandler = new AWSTopicHandler();


    MessageProducer producer;
    MessageConsumer consumer;
    MessageConsumer regConsumer;
    Session session;
    SQSConnection con;


    public void buildTopics(List<Stock> stocks) {
        stocks.stream().forEach(e -> e.setARN(this.topicHandler.buildTopic(e.getName())));
        stocks.stream().forEach(e -> System.out.println(e.getARN()));

    }

    private final MessageListener listener = new MessageListener() {
        @Override
        public void onMessage(Message msg) {
            if (msg instanceof ObjectMessage) {
                //TODO

            	try {
            		
            		BrokerMessage brokMsg = (BrokerMessage)((ObjectMessage) msg).getObject();
            		System.out.println("Broker: msg type = " + brokMsg.getType());
					
            		switch(brokMsg.getType()) {
						case STOCK_BUY:
							if (!isRegistered(msg.getStringProperty("name"))) {
								break;
							}
							BuyMessage buyMsg = (BuyMessage)((ObjectMessage) msg).getObject();
							buy(buyMsg.getStockName(), buyMsg.getAmount(), msg.getStringProperty("name"));
					    	break;
						case STOCK_SELL:
							if (!isRegistered(msg.getStringProperty("name"))) {
								break;
							}
							SellMessage sellMsg = (SellMessage)((ObjectMessage) msg).getObject();
							sell(sellMsg.getStockName(), sellMsg.getAmount(), msg.getStringProperty("name"));
							break;
						case STOCK_LIST:
							if (!isRegistered(msg.getStringProperty("name"))) {
								break;
							}
							System.err.println(stocks);
							ObjectMessage listMsg = session.createObjectMessage(new ListMessage(stocks));
							producer.send(listMsg);
							break;
						case SYSTEM_REGISTER:
							RegisterMessage regMsg = (RegisterMessage)((ObjectMessage) msg).getObject();
							Queue in = session.createQueue("asda");
							Queue out = session.createQueue("newQueue");
							
							if (clients.stream()
									.filter(c -> c.getClientName().equals(regMsg.getClientName()))
									.count() != 0 
							) {
								sendMessage("Client is already registered");
								break;
							}
							userStocksMap.put(
									regMsg.getClientName(), 
									new ArrayList<Stock>()
							);
							clients.add(new JmsBrokerClient(nextId++, regMsg.getClientName(), in, out));
							sendMessage("client has registered successfully");
							break;
						case SYSTEM_UNREGISTER: 
							UnregisterMessage unregMsg = (UnregisterMessage)((ObjectMessage) msg).getObject();
							if (!isRegistered(unregMsg.getClientName())) {
								break;
							}
							Iterator<JmsBrokerClient> it = clients.iterator();
							while (it.hasNext()) {
								JmsBrokerClient cl = it.next();
								if (cl.getClientName().equals(unregMsg.getClientName())) {
									it.remove();
								}
							}
							userStocksMap.remove(unregMsg.getClientName());
							sendMessage("client has unregistered successfully");
							break;
						case SYSTEM_QUIT:
							stop();
							break;
						default:
							sendMessage("Not a supported command.\n"
									+ "Please try one of this commands: list, sell, buy, register, unregister");
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

        buildTopics(stockList);
        this.topicHandler.publishTopic(stockList.get(0));

        /* TODO: initialize connection, sessions, etc. */
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

        this.session = this.con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // queue for polling registration requests
        this.regQueue = this.session.createQueue("RegistrationQueue");
        this.regConsumer = this.session.createConsumer(this.regQueue);
        this.regConsumer.setMessageListener(this.listener);
        
        Queue in = this.session.createQueue("asda");
        this.consumer = this.session.createConsumer(in);
        this.consumer.setMessageListener(this.listener);
        
        Queue out = this.session.createQueue("newQueue");
        this.producer = this.session.createProducer(out);
        this.con.start();
        
//        for(Stock stock : stockList) {
//            /* TODO: prepare stocks as topics */
//        	Topic topic = session.createTopic(stock.getName());
//        }

        this.stocks = stockList;
    }

    public boolean isRegistered(String clientName) throws JMSException {
        if (getClientByName(clientName) == null) {
            sendMessage("Client is not registered. Please register in prior.");
            return false;
        }
        return true;
    }

    public void stop() throws JMSException {
        //TODO
        System.out.println("request from broker server to stop the simple broker");
        this.con.close();
        System.exit(2);
    }

    public synchronized void buy(String stockName, int amount, String clientName) throws JMSException {

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
        		if (getStockByName(stockName, userStocksMap.get(clientName)) != null) {
        			for (int i = 0; i < clientStocks.size(); i++) {
        				if (clientStocks.get(i).getName().equals(stockName)) {
        					Stock updatedStock = clientStocks.get(i);
        					updatedStock.setAvailableCount(updatedStock.getAvailableCount() + amount);
        					clientStocks.set(i, updatedStock);
                			userStocksMap.put(clientName, clientStocks);
        				}
        			}
        		} else {
        		// user does not own the stock and has to be added to the map	
        			Stock newStock = new Stock(targetStock.getName(), amount, targetStock.getPrice());
        			userStocksMap.put(
        					clientName, 
        					Stream.concat(userStocksMap.get(clientName).stream(), Stream.of(newStock)).collect(Collectors.toCollection(ArrayList::new))
        			);
        		}
        		sendMessage(clientName + " has bought " + amount + " stocks of stock type " + stockName);
        		
        	} else {
        		sendMessage("number of requested stock is exceeded");
        	}
    	} else {
    		sendMessage("requested stock does not exist");
    	}
    }

    public JmsBrokerClient getClientByName(String clientName) {
        return this.clients.stream().filter(c -> c.getClientName().equals(clientName)).findFirst().orElse(null);
    }

    public Stock getStockByName(String stockName, List<Stock> stocks) {
        return stocks.stream().filter(s -> s.getName().equals(stockName)).findFirst().orElse(null);
    }

    public void sendMessage(String content) throws JMSException {
    	TextMessage msg = session.createTextMessage(content);
        this.producer.send(msg);
    }

	
    public synchronized void sell(String stockName, int amount, String clientName) throws JMSException {
        //TODO
    	Stock targetStock = getStockByName(stockName, stocks);
    	// stock exists (valid name)
    	if (targetStock != null) {
    		// client owns the stock 
    		if (getStockByName(stockName, this.userStocksMap.get(clientName)) != null) {
    			// client has enough of this stock type to sell
    			if (getStockByName(
    					stockName, this.userStocksMap.get(clientName)).getAvailableCount() >= amount
    			) {
    				// the maximum number of stocks is not exceeded
    				if (targetStock.getAvailableCount() + amount <= targetStock.getStockCount()) {
    					
    					targetStock.setAvailableCount(targetStock.getAvailableCount() + amount);
                		// replace old stock obj with updated stock obj
                		for (int i = 0; i < this.stocks.size(); i++) {
                			if (this.stocks.get(i).getName().equals(stockName)) {
                				stocks.set(i, targetStock);
                			}
                		}
                		// user owns the stock and is updated
                		List<Stock> clientStocks = userStocksMap.get(clientName);
                		for (int i = 0; i < clientStocks.size(); i++) {
                			if (clientStocks.get(i).getName().equals(stockName)) {
                				Stock updatedStock = clientStocks.get(i);
                				updatedStock.setAvailableCount(updatedStock.getAvailableCount() - amount);
                				clientStocks.set(i, updatedStock);
                        		userStocksMap.put(clientName, clientStocks);
                			}
                		}
                		sendMessage(clientName + " has sold " + amount + " stocks of stock type " + stockName);
    				} else {
    					sendMessage("maximum number of stocks is exceeded");
    				}
    			} else {
    				sendMessage("client does not own enough stocks of this type");
    			}
        	} else {
        		sendMessage("client does not own this stock. Please first buy stocks of this type to sell them.");
        	}
    	} else {
    		sendMessage("requested stock does not exist");
    	}
    }
    
    public synchronized List<Stock> getStockList() {
        List<Stock> stockList = new ArrayList<>();

        /* TODO: populate stockList */
        stockList.addAll(this.stocks);

        return stockList;
    }



}