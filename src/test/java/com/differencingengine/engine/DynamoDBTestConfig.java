package com.differencingengine.engine;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBTestConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBTestConfig.class);
    public static final DynamoDB DYNAMO_DB;
    public static final AmazonDynamoDB AMAZON_DYNAMO_DB;

    static {
        AMAZON_DYNAMO_DB = amazonDynamoDB();
        DYNAMO_DB = new DynamoDB(AMAZON_DYNAMO_DB);
    }

    protected static AmazonDynamoDB amazonDynamoDB() {
        try {
            LOG.info("Creating AmazonDynamoDB.");
            System.setProperty("sqlite4java.library.path", "./libs/"); // This is necessary for some stupid reason.
            return DynamoDBEmbedded.create().amazonDynamoDB();
        } catch (Exception e) {
            LOG.error("Unable to create local DynamoDB.");
            e.printStackTrace();
            return null;
        }
    }
}
