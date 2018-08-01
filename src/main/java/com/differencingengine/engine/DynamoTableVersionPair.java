package com.differencingengine.engine;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DynamoTableVersionPair {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoTableVersionPair.class);

    private String tableName;
    private CreateTableRequest localCreateTableRequest;
    private DescribeTableResult remoteDescribeTableResult;
    private Map<String, GlobalSecondaryIndex> localTableIndexMapping  = new HashMap<>();
    private Map<String, GlobalSecondaryIndexDescription> remoteTableIndexMapping = new HashMap<>();

    /**
     * Represents a pair of identically named Dynamo DB tables to potentially reconcile differences between.
     * One should be the local version of the table which is the latest version and the other should be
     * the remote version of the table as it stands on Amazon's servers.
     * @param localCreateTableRequest The local createTableRequest as generated in InitializationLambdaContainer.java.
     * @param remoteDescribeTableResult The remote DescribeTableResult as returned by Amazon's DescribeTable API.
     */
    DynamoTableVersionPair(CreateTableRequest localCreateTableRequest, DescribeTableResult remoteDescribeTableResult) {
        if (localCreateTableRequest == null) {
            LOG.error("Cannot instantiate DynamoTableVersionPair object with null CreateTableRequest. There must always be a local instance of the table.");
            throw new IllegalArgumentException("Cannot instantiate DynamoTableVersionPair object with null CreateTableRequest. There must always be a local instance of the table.");
        }

        if (remoteDescribeTableResult != null && (localCreateTableRequest.getTableName().compareTo(remoteDescribeTableResult.getTable().getTableName()) != 0)) {
            LOG.error("Cannot create a DynamoTableVersionPair between two tables with different names. Local table name: ({}). Remote table table: ({}).", localCreateTableRequest.getTableName(), remoteDescribeTableResult.getTable().getTableName());
        }

        tableName = localCreateTableRequest.getTableName();

        LOG.info("Creating new DynamoTableVersionPair with localCreateTableRequest ({}) and remoteDescribeTableResult ({}).", localCreateTableRequest, remoteDescribeTableResult);

        this.localCreateTableRequest = localCreateTableRequest;
        this.remoteDescribeTableResult = remoteDescribeTableResult;

        if (this.localCreateTableRequest.getGlobalSecondaryIndexes() != null) {
            for (GlobalSecondaryIndex globalSecondaryIndex: this.localCreateTableRequest.getGlobalSecondaryIndexes()) {
                localTableIndexMapping.put(globalSecondaryIndex.getIndexName(), globalSecondaryIndex);
            }

            LOG.info("Successfully mapped ({}) indexes to local table ({}).", localTableIndexMapping.size(), localCreateTableRequest.getTableName());
        } else {
            LOG.info("Did not map any local indexes because the table didn't have any.");
        }

        if (remoteDescribeTableResult != null) {
            if (this.remoteDescribeTableResult.getTable().getGlobalSecondaryIndexes() != null) {
                for (GlobalSecondaryIndexDescription globalSecondaryIndexDescription: this.remoteDescribeTableResult.getTable().getGlobalSecondaryIndexes()) {
                    remoteTableIndexMapping.put(globalSecondaryIndexDescription.getIndexName(), globalSecondaryIndexDescription);
                }
                LOG.info("Successfully mapped ({}) indexes to remote table ({}).", remoteTableIndexMapping.size(), this.remoteDescribeTableResult.getTable().getTableName());
            } else {
                LOG.info("Did not map any remote indexes because the table didn't have any.");
            }
        } else {
            LOG.info("Did not map any remote indexes because the table does not exist on AWS yet.");
        }
    }

    /**
     * Computes the list of GlobalSecondaryIndexes which exists locally but not remotely.
     * @return A list of GlobalSecondaryIndexes to add to this pair's remote table definition.
     */
    public Map<String, GlobalSecondaryIndex> getIndexesToCreate() {
        if (requiresCreation()) {
            LOG.info("localCreateTableRequest ({}) and remoteDescribeTableResult ({}) do not necessitate creating indexes.", localTableIndexMapping, remoteDescribeTableResult);
            return new HashMap<>();
        }

        Map<String, GlobalSecondaryIndex> indexesToCreate = new HashMap<>();

        for (String indexName: localTableIndexMapping.keySet()) {
            if (!remoteTableIndexMapping.containsKey(indexName)) {
                indexesToCreate.put(indexName, localTableIndexMapping.get(indexName));
                LOG.info("Mapped name of Index ({}) to Index ({}) for creation.", indexName, localTableIndexMapping.get(indexName));
            }
        }

        LOG.info("Successfully mapped ({}) new Indexes for creation.", indexesToCreate.size());
        return indexesToCreate;
    }

    /**
     * Computes the list of GlobalSecondaryIndexes which exists remotely but not locally.
     * @return A list of GlobalSecondaryIndexes to delete from this pair's remote table definition.
     */
    public Map<String, GlobalSecondaryIndex> getIndexesToDelete() {
        if (localCreateTableRequest == null || remoteDescribeTableResult == null) {
            return new HashMap<>();
        }

        Map<String, GlobalSecondaryIndex> indexesToDelete = new HashMap<>();

        for (String indexName: remoteTableIndexMapping.keySet()) {
            if (!localTableIndexMapping.containsKey(indexName)) {
                GlobalSecondaryIndex toDelete = convertDescriptionToIndex(remoteTableIndexMapping.get(indexName));
                indexesToDelete.put(indexName, toDelete);
                LOG.info("Mapped name of Index ({}) to Index ({}) for deletion.", indexName, toDelete);
            }
        }

        LOG.info("Successfully mapped ({}) old Indexes for deletion.", indexesToDelete.size());
        return indexesToDelete;
    }

    private GlobalSecondaryIndex convertDescriptionToIndex(GlobalSecondaryIndexDescription description) {
        GlobalSecondaryIndex index = new GlobalSecondaryIndex();

        index.setIndexName(description.getIndexName());
        index.setKeySchema(description.getKeySchema());
        index.setProjection(description.getProjection());

        ProvisionedThroughput throughput = new ProvisionedThroughput()
                .withReadCapacityUnits(
                        description.getProvisionedThroughput().getReadCapacityUnits())
                .withWriteCapacityUnits(
                        description.getProvisionedThroughput().getWriteCapacityUnits());
        index.setProvisionedThroughput(throughput);

        LOG.info("Converted GlobalSecondaryIndexDescription ({}) to GlobalSecondaryIndex ({}).", description, index);
        return index;
    }

    /**
     * If there is no remoteDescribeTableResult then the table doesn't exist.
     * If there is a localCreateTableRequest then the table should exist.
     * @return True if we need to create the table from scratch, false otherwise.
     */
    public boolean requiresCreation() {
        return remoteDescribeTableResult == null;
    }

    /**
     * @return True iff the local table and remote table both exist and differ from each other.
     */
    public boolean requiresModification() {
        return !requiresCreation() && ((getIndexesToCreate().size() > 0) || (getIndexesToDelete().size() > 0));
    }

    public String getTableName() {
        return tableName;
    }

    public CreateTableRequest getCreateTableRequest() {
        return localCreateTableRequest;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tableName: ");
        sb.append(tableName);
        sb.append("\n");
        sb.append("localCreateTableRequest: ");
        sb.append(localCreateTableRequest);
        sb.append("\n");
        sb.append("remoteDescribeTableResult: ");
        sb.append(remoteDescribeTableResult);
        sb.append("remoteTableIndexMapping: ");
        sb.append(remoteTableIndexMapping);
        sb.append("\n");
        sb.append("localTableIndexMapping: ");
        sb.append(localTableIndexMapping);
        sb.append("\n");
        return sb.toString();
    }
}
