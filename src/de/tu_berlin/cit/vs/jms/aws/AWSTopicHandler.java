package de.tu_berlin.cit.vs.jms.aws;


import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import de.tu_berlin.cit.vs.jms.common.Stock;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AWSTopicHandler {

    ArrayList<String> topics;
    public ArrayList<CreateTopicResult> arns;
    AWSConnection awsConnection = new AWSConnection();

    public AWSTopicHandler(ArrayList<String> topics) {
        this.topics = topics;
        ArrayList arns = this.topics.stream().map( e -> buildTopic(e)).collect(Collectors.toCollection(ArrayList::new));
        this.arns = arns; //stupid
    }

    public AWSTopicHandler() {}


    public void publishTopic(Stock stock) {
        //publish to an SNS topic
        String msg = "My text published to SNS topic with email endpoint";
        System.out.println("--");
        System.out.println(stock.getARN().toString());
        System.out.println("--");
        PublishRequest publishRequest = new PublishRequest(stock.getARN(). getTopicArn(), stock.toString());
        PublishResult publishResult = awsConnection.snsClient.publish(publishRequest);
        /* print MessageId of message published to SNS topic */
        System.out.println("MessageId - " + publishResult.getMessageId());
    }

    public CreateTopicResult buildTopic(String topicName) {

        //create a new SNS topic
        CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
        CreateTopicResult createTopicResult = awsConnection.snsClient.createTopic(createTopicRequest);
        //print TopicArn
        //System.out.println(createTopicResult);
        //get request id for CreateTopicRequest from SNS metadata
        //System.out.println("CreateTopicRequest - " + snsClient.getCachedResponseMetadata(createTopicRequest));
        return createTopicResult;
    }





}
