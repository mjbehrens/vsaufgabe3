package de.tu_berlin.cit.vs.jms.broker;

import java.util.ArrayList;
import java.util.List;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import de.tu_berlin.cit.vs.jms.common.BuyMessage;
import de.tu_berlin.cit.vs.jms.common.ListMessage;
import de.tu_berlin.cit.vs.jms.common.RequestListMessage;
import de.tu_berlin.cit.vs.jms.common.SellMessage;
import de.tu_berlin.cit.vs.jms.common.Stock;
import org.apache.activemq.ActiveMQConnectionFactory;


public class SimpleBroker {
    /* TODO: variables as needed */
	List<Stock> stocks = new ArrayList<>();
	MessageProducer producer;
	MessageConsumer consumer;
	Session session;
    
    private final MessageListener listener = new MessageListener() {
        @Override
        public void onMessage(Message msg) {
            if(msg instanceof ObjectMessage) {
                //TODO
            	try {
					switch(((ObjectMessage) msg).getObject().toString()) {
					case "BuyMessage":
						System.out.println(((ObjectMessage) msg).getObject().toString());
						BuyMessage buyMsg = (BuyMessage)((ObjectMessage) msg).getObject();
						int index = buy(buyMsg.getStockName(), buyMsg.getAmount());
						Stock targetStock = stocks.get(index);
						targetStock.setAvailableCount(targetStock.getAvailableCount() - 1);
				    	targetStock.setStockCount(targetStock.getStockCount() +  1);
						break;
					case "SellMessage":
						SellMessage sellMsg = (SellMessage)((ObjectMessage) msg).getObject();
						sell(sellMsg.getStockName(), sellMsg.getAmount());
						break;
					case "RequestListMessage":
						ObjectMessage listMsg = session.createObjectMessage(new ListMessage(stocks));
						producer.send(listMsg);
						break;
						
					}
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            	
            }
        }
    };
    
    public SimpleBroker(List<Stock> stockList) throws JMSException {
        /* TODO: initialize connection, sessions, etc. */
        ActiveMQConnectionFactory conFactory = new ActiveMQConnectionFactory(
        		"tcp://localhost:61616");
        Connection con = conFactory.createConnection();
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
    }
    
    public void stop() throws JMSException {
        //TODO
    	System.out.println("request from broker server to stop the simple broker");
    	System.exit(2);
    }
    
    public synchronized int buy(String stockName, int amount) throws JMSException {
        //TODO
    	/*
    	Stock targetStock = stocks.stream().filter(s -> s.getName() == stockName)
    			.findFirst().get();
    	targetStock.setAvailableCount(targetStock.getAvailableCount() - 1);
    	targetStock.setStockCount(targetStock.getStockCount() +  1);
    	this.stocks.stream().filter(s -> s.getName().equals(stockName)).findFirst()*/
    	
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
