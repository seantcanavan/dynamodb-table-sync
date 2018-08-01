package com.differencingengine.engine;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalSecondaryIndexAction;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.IndexStatus;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;


/**
 * Accepts a set of DynamoTables that are defined locally in Java code through CreateTableRequests as well as
 * a set of remote DynamoTables that are defined remotely on AWS' server through DescribeTableRequests. For each
 * local table a match is attempted to be found by name in the remote set of tables. If no remote match exists, then
 * a new remote table is created to match the local table. In the case where a remote table is found that matches a
 * local table then a DynamoTableVersionPair is created to compare the two tables. The difference between any
 * GlobalSecondaryIndexes found are computed and automatically reconciled. GlobalSecondaryIndexes that are defined
 * locally but not remotely on the matching table are created. GlobalSecondaryIndexes that are defined remotely but not
 * locally on the matching table are deleted.
 *
 * When this class is finished operating all missing tables will be created, and existing tables will be updated only
 * if necessary. This set of operations should be safe to perform each subsequent deploy via utilities like Chef,
 * Puppet, CodeShip, Jenkins, etc. This means you can run it every time you deploy without worrying about it making
 * unnecessary changes.
 */
public class DynamoTableDifferencingEngine {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoTableDifferencingEngine.class);

    private final DynamoDB dynamoDB;
    private final AmazonDynamoDB amazonDynamoDB;

    private List<DynamoTableVersionPair> matchingDynamoTablePairs;
    private Map<String, CreateTableRequest> createTableRequestDefinitions;

    private static final int HASH_ONLY = 1;
    private static final int HASH_AND_RANGE = 2;
    private final int delayBetweenSuccessiveQueriesMillis;

    private final Semaphore lock;

    public DynamoTableDifferencingEngine(AmazonDynamoDB amazonDynamoDB, DynamoDB dynamoDB, int delayBetweenSuccessiveQueriesMillis) {
        this.dynamoDB = dynamoDB;
        this.lock = new Semaphore(1);
        this.amazonDynamoDB = amazonDynamoDB;
        this.delayBetweenSuccessiveQueriesMillis = delayBetweenSuccessiveQueriesMillis;
    }

    /**
     * Accepts a map of table names to their respective create table requests. Will use the table names provided to
     * query AWS and look for any matching tables that may or may not exist. Any differences between the remote set of
     * tables returned after querying and the local set of tables provided by the user is automatically reconciled.
     * @param localDynamoTableMap  The mapping of tables that you wish to evaluate. from TableName -> CreateTableRequest
     */
    public void synchronizeLocalAgainstRemote(Map<String, CreateTableRequest> localDynamoTableMap) throws InterruptedException {
        //We don't want to run this program more than once at a time since it involves waiting and modifying the more tables.
        lock.acquire();

        //Since this is a singleton bean we need to clear this list before executing just in case.
        matchingDynamoTablePairs = Lists.newLinkedList();
        createTableRequestDefinitions = Maps.newHashMap();

        LOG.info("Received ({}) CreateTableRequests representing the set of DynamoTables that currently exist locally.", localDynamoTableMap);
        this.createTableRequestDefinitions.putAll(localDynamoTableMap);

        // Create a 1-1 mapping between local tables and remote tables based on the exact table name.
        // Any matches will be compared against each other to reconcile differences.
        // Any misses will cause the table to be created.
        for (String tableName: localDynamoTableMap.keySet()) {
            CreateTableRequest localTable = localDynamoTableMap.get(tableName);
            DescribeTableResult remoteTable = null;
            try {
                remoteTable = amazonDynamoDB.describeTable(tableName);
            } catch (ResourceNotFoundException ignored) { } // it doesn't matter if the table exists or not. The DynamoTableVersionPair constructor allows it to be null.

            matchingDynamoTablePairs.add(new DynamoTableVersionPair(localTable, remoteTable)); // passing null is fine when the table is missing
            if (remoteTable != null) {
                LOG.info("Successfully matched local table ({}) with remote table ({}) based off of name.", localTable, remoteTable);
            } else {
                LOG.info("No matching remote table found for ({}). It will be created from scratch.", tableName);
            }
        }

        for (DynamoTableVersionPair currentPair: matchingDynamoTablePairs) {
            LOG.info("Currently processing DynamoTableVersionPair ({}).", currentPair);

            if (currentPair.requiresCreation()) {
                LOG.info("The current DynamoTableVersionPair has no matching remote counterpart. It will be created.");
                TableUtils.createTableIfNotExists(amazonDynamoDB, currentPair.getCreateTableRequest());
                LOG.info("Successfully created table with name ({}).", currentPair.getTableName());
                waitForTableCreation(currentPair.getTableName());
            }

            else if (currentPair.requiresModification()) {
                LOG.info("The current DynamoTableVersionPair has a discrepancy between local and remote indexes. These will be reconciled.");

                Map<String, GlobalSecondaryIndex> oldIndexMap = currentPair.getIndexesToDelete();
                if (oldIndexMap.size() > 0) {
                    deleteSuperfluousIndexes(currentPair.getTableName(), oldIndexMap);
                    LOG.info("Successfully deleted ({}) superfluous indexes for table ({}).", oldIndexMap.size(), currentPair.getTableName());
                }

                Map<String, GlobalSecondaryIndex> newIndexMap = currentPair.getIndexesToCreate();
                if (newIndexMap.size() > 0) {
                    createMissingIndexes(currentPair.getTableName(), newIndexMap);
                    LOG.info("Successfully created ({}) new indexes for table ({}).", newIndexMap.size(), currentPair.getTableName());
                }
            }

            else {
                LOG.info("The two versions of the table are identical. No changes to reconcile.");
            }
        }

        //Release our lock for the next thread.
        lock.release();
    }

    private void createMissingIndexes(String tableName, Map<String, GlobalSecondaryIndex> newIndexMap) throws InterruptedException {
        LOG.info("Creating ({}) new indexes that are missing from the remote table ({}).", newIndexMap.size(), tableName);
        for(String indexName: newIndexMap.keySet()) {
            GlobalSecondaryIndex newGsi = newIndexMap.get(indexName);
            LOG.info("Current processing new GSI request for GSI ({}).", newGsi);

            CreateGlobalSecondaryIndexAction createGsiAction = new CreateGlobalSecondaryIndexAction()
                    .withIndexName(newGsi.getIndexName())
                    .withKeySchema(newGsi.getKeySchema())
                    .withProjection(newGsi.getProjection())
                    .withProvisionedThroughput(newGsi.getProvisionedThroughput());
            LOG.info("Generated CreateGlobalSecondaryIndexAction ({}).", createGsiAction);


            Table table = dynamoDB.getTable(tableName);
            int keyElements = newGsi.getIndexName().split("-").length -1; // expecting one of: "user-created-index" or "user-index" for hash + range or just hash, respectively.
            LOG.info("Detected ({}) potential keyElements.", keyElements);

            if (keyElements < HASH_ONLY) {
                LOG.error("Expecting at least 1 key schema element for GlobalSecondaryIndex index name ({}). Check your index name and make sure it follows AWS convention 'hash-range-index' where the range key is optional.", newGsi.getIndexName());
                throw new IllegalArgumentException(String.format("Expecting at least 1 key schema element for GlobalSecondaryIndex index name (%s). Check your index name and make sure it follows AWS convention 'hash-range-index' where the range key is optional.", newGsi.getIndexName()));
            }

            else if (keyElements == HASH_ONLY) {
                LOG.info("keyElements is equal to ({}). Assuming the user wants just a hashKey.", HASH_ONLY);
                String hashKey = newGsi.getIndexName().split("-")[0];
                LOG.info("Proceeding with hashKey as ({}).", hashKey);
                AttributeDefinition hashDefinition = null;

                for (AttributeDefinition currentAttribute: this.createTableRequestDefinitions.get(tableName).getAttributeDefinitions()) {
                    if (currentAttribute.getAttributeName().compareToIgnoreCase(hashKey) == 0) {
                        hashDefinition = currentAttribute;
                        LOG.info("Successfully matched AttributeDefinition ({}) to hashKey ({}). Can continue processing new GSI request.", hashDefinition);
                    }
                }

                if (hashDefinition == null) {
                    LOG.error("Expecting exactly 1 AttributeDefinition.name to match the index name ({}) but none were found. Please double check your AttributeDefinition list ({}) and index name and try again.", hashKey, this.createTableRequestDefinitions.get(tableName).getAttributeDefinitions());
                    throw new IllegalArgumentException(String.format("Expecting exactly 1 AttributeDefinition.name to match the index name (%s) but none were found. Please double check your AttributeDefinition list (%s) and index name and try again.", hashKey, this.createTableRequestDefinitions.get(tableName).getAttributeDefinitions()));
                }

                Index createdIndex = table.createGSI(createGsiAction, hashDefinition);
                LOG.info("Successfully issued createGSI request to Amazon and received Index ({}) as a response.", createdIndex);
                waitForCreateIndex(createdIndex);
                LOG.info("Successfully waited for Index ({}) to be initialized.", createdIndex);
            }

            else if (keyElements == HASH_AND_RANGE) {
                LOG.info("keyElements is equal to ({}). Assuming the user wants both a hashKey and a rangeKey.", HASH_AND_RANGE);
                String hashKey = newGsi.getIndexName().split("-")[0];
                String rangeKey = newGsi.getIndexName().split("-")[1];
                AttributeDefinition hashDefinition = null;
                AttributeDefinition rangeDefinition = null;

                for (AttributeDefinition currentAttribute: this.createTableRequestDefinitions.get(tableName).getAttributeDefinitions()) {
                    if (currentAttribute.getAttributeName().compareToIgnoreCase(hashKey) == 0) {
                        hashDefinition = currentAttribute;
                    }
                    if (currentAttribute.getAttributeName().compareToIgnoreCase(rangeKey) == 0) {
                        rangeDefinition = currentAttribute;
                    }
                }

                if (hashDefinition == null || rangeDefinition == null) {
                    LOG.error("Expecting exactly 1 AttributeDefinition.name to match the hashKey definition ({}) and a second AttributeDefinition.name to match the the rangeKey definition ({}) but one or both were not found. Please double check your AttributeDefinition list ({}) and index name and try again.", hashKey, rangeKey, this.createTableRequestDefinitions.get(tableName).getAttributeDefinitions());
                    throw new IllegalArgumentException(String.format("Expecting exactly 1 AttributeDefinition.name to match the hashKey definition (%s) and a second AttributeDefinition.name to match the the rangeKey definition (%s) but one or both were not found. Please double check your AttributeDefinition list (%s) and index name and try again.", hashKey, rangeKey, this.createTableRequestDefinitions.get(tableName).getAttributeDefinitions()));
                }

                Index createdIndex = table.createGSI(createGsiAction, hashDefinition, rangeDefinition);
                LOG.info("Successfully issued createGSI request to Amazon and received Index ({}) as a response.", createdIndex);
                waitForCreateIndex(createdIndex);
                LOG.info("Successfully waited for Index ({}) to be initialized.", createdIndex);
            }

            else {
                LOG.error("Expecting no more than 2 key schema elements for GlobalSecondaryIndex index name (%s). Check your index name and make sure it follows AWS convention 'hash-range-index' where the range key is optional.", newGsi.getIndexName());
                throw new IllegalArgumentException(String.format("Expecting no more than 2 key schema elements for GlobalSecondaryIndex index name (%s). Check your index name and make sure it follows AWS convention 'hash-range-index' where the range key is optional.", newGsi.getIndexName()));
            }
        }
    }

    private void deleteSuperfluousIndexes(String tableName, Map<String, GlobalSecondaryIndex> deleteIndexMap) throws InterruptedException {
        LOG.info("Deleting ({}) superfluous indexes from Table ({}).", deleteIndexMap.size(), tableName);
        for(String indexName: deleteIndexMap.keySet()) {
            LOG.info("Working on deleting Index ({}) for Table ({}).", indexName, tableName);
            Index index = dynamoDB.getTable(tableName).getIndex(indexName);
            LOG.info("Successfully retrieved Index name ({}) from AmazonDynamo.", index.getIndexName());
            index.deleteGSI();
            waitForDeleteIndex(index);
            LOG.info("Done waiting for Index ({}) to delete.", index.getIndexName());
        }
        LOG.info("Finished processing all ({}) indexes to delete. Returning.", deleteIndexMap.size());
    }

    private void waitForCreateIndex(Index newIndex) throws InterruptedException {
        LOG.info("Waiting for IndexStatus to be ACTIVE for Index ({}).", newIndex);
        LOG.info("Waiting for parent table ({}) to be ACTIVE before proceeding.", newIndex.getTable().getTableName());
        waitForTableCreation(newIndex.getTable().getTableName());
        LOG.info("Successfully verified that the parent table ({}) is ACTIVE and ready for modification.", newIndex.getTable().getTableName());
        boolean finishedWaiting = false;
        while (!finishedWaiting) {
            DescribeTableResult description = amazonDynamoDB.describeTable(newIndex.getTable().getTableName());
            for (GlobalSecondaryIndexDescription currentGsi: description.getTable().getGlobalSecondaryIndexes()) {
                if (currentGsi.getIndexName().equals(newIndex.getIndexName())) {
                    IndexStatus indexStatus = IndexStatus.fromValue(currentGsi.getIndexStatus());
                    LOG.info("Successfully located Index with name ({}) and parsed its status as ({}).", currentGsi.getIndexName(), indexStatus);
                    if (indexStatus.equals(IndexStatus.ACTIVE)) {
                        LOG.info("The Index with name ({}) has status ACTIVE. Returning.", currentGsi.getIndexName());
                        return;
                    } else if(indexStatus.equals(IndexStatus.UPDATING) || indexStatus.equals(IndexStatus.CREATING)) {
                        LOG.info("The Index with name ({}) has status UPDATING or CREATING. Attempting to sleep.", currentGsi.getIndexName());
                        try {
                            LOG.info("Attempting to sleep for ({}) milliseconds while waiting for Index ({}) to be ACTIVE.", delayBetweenSuccessiveQueriesMillis, currentGsi.getIndexName());
                            Thread.sleep(delayBetweenSuccessiveQueriesMillis);
                            LOG.info("Successfully slept for ({}) milliseconds while waiting for Index ({}) to be ACTIVE.", delayBetweenSuccessiveQueriesMillis, currentGsi.getIndexName());
                        } catch (InterruptedException e) {
                            LOG.error("We are being interrupted while waiting for Index ({}) to be created. We are exiting the program in a hasty manner as a result.", currentGsi.getIndexName(), e);
                            throw e;
                        }
                    } else if(indexStatus.equals(IndexStatus.DELETING)) {
                        LOG.error("Index ({}) was changed to DELETING somehow!", currentGsi);
                        throw new IllegalArgumentException(String.format("Index (%s) was changed to DELETING somehow!", currentGsi));
                    } else {
                        LOG.error("There was a new value added to enum IndexStatus of type ({}) and it needs to be accounted for in this if / else branch.", indexStatus);
                        throw new IllegalArgumentException(String.format("There was a new value added to enum IndexStatus of type (%s) and it needs to be accounted for in this if / else branch.", indexStatus));
                    }
                }
            }
        }
    }

    private void waitForDeleteIndex(Index oldIndex) throws InterruptedException {
        LOG.info("Waiting for IndexStatus to move from DELETING status and leave the table description altogether for Index ({}).", oldIndex);
        LOG.info("Waiting for parent table ({}) to be ACTIVE before proceeding.", oldIndex.getTable().getTableName());
        waitForTableCreation(oldIndex.getTable().getTableName());
        LOG.info("Successfully verified that the parent table ({}) is ACTIVE and ready for modification.", oldIndex.getTable().getTableName());
        boolean finishedWaiting = false;
        boolean indexExists = false;
        while (!finishedWaiting) {
            DescribeTableResult description = amazonDynamoDB.describeTable(oldIndex.getTable().getTableName());
            List<GlobalSecondaryIndexDescription> existingIndexes = description.getTable().getGlobalSecondaryIndexes();
            if (existingIndexes == null || existingIndexes.size() == 0) {
                LOG.info("No more indexes left to check therefore we don't need to wait.");
                return;
            }
            indexExists = false;
            for (GlobalSecondaryIndexDescription currentGsi: description.getTable().getGlobalSecondaryIndexes()) {
                if (currentGsi.getIndexName().equals(oldIndex.getIndexName())) {
                    IndexStatus indexStatus = IndexStatus.fromValue(currentGsi.getIndexStatus());
                    LOG.info("Successfully located Index with name ({}) and parsed its status as ({}).", currentGsi.getIndexName(), indexStatus);
                    indexExists = true;
                    if (indexStatus.equals(IndexStatus.DELETING)) {
                        LOG.info("The Index with name ({}) has status DELETING. Attempting to sleep.", currentGsi.getIndexName());
                        try {
                            LOG.info("Attempting to sleep for ({}) milliseconds while waiting for Index ({}) to disappear from table description.", delayBetweenSuccessiveQueriesMillis, currentGsi.getIndexName());
                            Thread.sleep(delayBetweenSuccessiveQueriesMillis);
                            LOG.info("Successfully slept for ({}) milliseconds while waiting for Index ({}) to disappear from table description.", delayBetweenSuccessiveQueriesMillis, currentGsi.getIndexName());
                        } catch (InterruptedException e) {
                            LOG.error("We are being interrupted while waiting for Index ({}) to delete. We are exiting the program in a hasty manner as a result.", currentGsi.getIndexName(), e);
                            throw e;
                        }
                    } else {
                        LOG.error("Index ({}) was changed from DELETING to a new status! This definitely should not happen.", currentGsi);
                        throw new IllegalArgumentException(String.format("Index (%s) was changed from DELETING to a new status! This definitely should not happen.", currentGsi));
                    }
                }
            }
            if (!indexExists) {
                LOG.info("The Index with name ({}) could not be located in the table's list of GlobalSecondaryIndexes ({}). Therefore it is deleted. Returning.", oldIndex.getIndexName(), existingIndexes);
                finishedWaiting = true;
            }
        }
    }

    private void waitForTableCreation(String tableName) throws InterruptedException {
        LOG.info("Waiting for Table ({}) to be created.", tableName);
        try {
            TableUtils.waitUntilActive(amazonDynamoDB, tableName);
            LOG.info("Done waiting for Table ({}) to be created and initialized.", tableName);
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for Table ({}) to be created. Please deploy again later to finish reconciling any differences between local and remote tables.", tableName, e);
            throw e;
        }
    }
}
