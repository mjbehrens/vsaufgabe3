package de.tu_berlin.cit.vs.jms.broker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.*;

import javax.jms.Destination;
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
import de.tu_berlin.cit.vs.jms.common.RequestListMessage;
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

                    BrokerMessage brokMsg = (BrokerMessage) ((ObjectMessage) msg).getObject();
                    System.out.println("Broker: msg type = " + brokMsg.getType());

                    switch (brokMsg.getType()) {
                        case STOCK_BUY:
                            if (!isRegistered(msg.getStringProperty("name"))) {
                                break;
                            }
                            BuyMessage buyMsg = (BuyMessage) ((ObjectMessage) msg).getObject();
                            System.out.println("name: " + msg.getStringProperty("name"));
                            buy(buyMsg.getStockName(), buyMsg.getAmount(), msg.getStringProperty("name"));
                            break;
                        case STOCK_SELL:
                            if (!isRegistered(msg.getStringProperty("name"))) {
                                break;
                            }
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
                            if (!isRegistered(msg.getStringProperty("name"))) {
                                break;
                            }
                            ObjectMessage listMsg = session.createObjectMessage(new ListMessage(stocks));
                            producer.send(listMsg);
                            break;
                        case SYSTEM_REGISTER:
                            RegisterMessage regMsg = (RegisterMessage) ((ObjectMessage) msg).getObject();
                            if (isRegistered(regMsg.getClientName())) {
                                sendMessage("Client is already registered");
                                break;
                            }

                            Queue in = session.createQueue("asda");
                            Queue out = session.createQueue("newQueue");
                            initializeQueues(in, out);
                            userStocksMap.put(
                                    regMsg.getClientName(),
                                    new ArrayList<Stock>()
                            );
                            clients.add(new JmsBrokerClient(nextId++, regMsg.getClientName(), in, out));
                            sendMessage("client has registered successfully");
                            break;
                        case SYSTEM_UNREGISTER:
                            UnregisterMessage unregMsg = (UnregisterMessage) ((ObjectMessage) msg).getObject();
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
        this.regQueue = session.createQueue("RegistrationQueue");
        this.regConsumer = session.createConsumer(regQueue);
        this.regConsumer.setMessageListener(this.listener);
        this.con.start();
//        for(Stock stock : stockList) {
//            /* TODO: prepare stocks as topics */
//        	Topic topic = session.createTopic(stock.getName());
//        }

        this.stocks = stockList;
    }

    public void initializeQueues(Queue in, Queue out) throws JMSException {
        this.producer = this.session.createProducer(out);
        this.consumer = this.session.createConsumer(in);
        this.consumer.setMessageListener(listener);

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
        // client exists (is registered)
        if (getClientByName(clientName) == null) {
            sendMessage("cannot invoke buy or sell. Please register in prior!");
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
                sendMessage("number of requested stock");
            }
        } else {
            sendMessage("requested stock does not exist");
        }
    }

    public JmsBrokerClient getClientByName(String clientName) {
        return clients.stream().filter(c -> c.getClientName().equals(clientName)).findFirst().orElse(null);
    }

    public Stock getStockByName(String stockName, List<Stock> stocks) {
        return stocks.stream().filter(s -> s.getName().equals(stockName)).findFirst().orElse(null);
    }

    public void sendMessage(String content) throws JMSException {
        TextMessage msg = session.createTextMessage(content);
        this.producer.send(msg);
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
        for (int i = 0; i < this.stocks.size(); i++) {
            if (stocks.get(i).getName().equals(stockName)) {

                if (stocks.get(i).getAvailableCount() + amount > stocks.get(i).getStockCount()) {
                    System.out.println("sell impossible, total amount of stocks exceeded the initial stock count");
                    sendMessage("sell impossible, total amount of stocks exceeded the initial stock count");
                    return false;
                } else {
                    stocks.get(i).setAvailableCount(stocks.get(i).getAvailableCount() + amount);
                    return true;

                }
            }
        }
        // if it doesn't find the stock in the stocks list
        sendMessage("requested stock does not exist");
        return false;
    }

    public synchronized List<Stock> getStockList() {
        List<Stock> stockList = new ArrayList<>();

        /* TODO: populate stockList */
        stockList.addAll(this.stocks);

        return stockList;
    }
}