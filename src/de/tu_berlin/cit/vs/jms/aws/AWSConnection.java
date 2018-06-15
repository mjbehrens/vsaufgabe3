package de.tu_berlin.cit.vs.jms.aws;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;


public class AWSConnection {

    public AmazonSNSClient snsClient;


    public AWSConnection() {
        String ACCESS_KEY = "AKIAIBTHVB24KIISRKRQ";
        String SECRET_KEY = "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";

        BasicAWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);

        //create a new SNS client and set endpoint
        this.snsClient = new AmazonSNSClient(credentials);
        this.snsClient.setRegion(Region.getRegion(Regions.US_EAST_2));
    }
}
