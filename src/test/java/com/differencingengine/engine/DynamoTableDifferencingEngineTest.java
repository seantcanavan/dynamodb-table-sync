package com.differencingengine.engine;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.util.List;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.unitils.reflectionassert.ReflectionAssert.assertReflectionEquals;

public class DynamoTableDifferencingEngineTest {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoTableDifferencingEngineTest.class);

    private static final String SAFE_PREFIX = "dev_";
    private static final int DELAY_BETWEEN_SUCCESSIVE_QUERIES_MILLIS = 500; // wait half a second. any less and we spam the logs. any more and we delay the unit tests.

    private static DynamoDB dynamoDB = DynamoDBTestConfig.DYNAMO_DB;
    private static AmazonDynamoDB amazonDynamoDB = DynamoDBTestConfig.AMAZON_DYNAMO_DB;
    private static DynamoTableDifferencingEngine differencingEngine;

    @BeforeSuite
    public void beforeClass() {
        LOG.info("Setting up.");
        differencingEngine = new DynamoTableDifferencingEngine(amazonDynamoDB, dynamoDB, DELAY_BETWEEN_SUCCESSIVE_QUERIES_MILLIS);
    }

    @AfterMethod
    public void afterMethod() {
        LOG.info("Tearing down.");
        amazonDynamoDB.listTables().getTableNames().forEach(table -> {
            if (table.startsWith(SAFE_PREFIX)) {
                LOG.info("Deleting Table({}).", table);
                amazonDynamoDB.deleteTable(table);
            } else {
                throw new RuntimeException("You may only programmatically delete a table that starts with the safe prefix.");
            }
        });
    }

    @Test
    public void shouldCreateTableWithHashOnly() throws Exception {
        CreateTableRequest request = CreateTableRequestTestBuilder.valid().withHashKeyOnly().build();
        Map<String, CreateTableRequest> mapping = Maps.newHashMap();
        mapping.put(request.getTableName(), request);
        differencingEngine.synchronizeLocalAgainstRemote(mapping);

        DescribeTableResult describeTableResult = amazonDynamoDB.describeTable(request.getTableName());
        TableDescription tableDescription = describeTableResult.getTable();
        assertThat(tableDescription.getTableStatus()).isEqualTo("ACTIVE");
        assertThat(tableDescription.getGlobalSecondaryIndexes()).isNull();
        assertThat(tableDescription.getLocalSecondaryIndexes()).isNull();

        assertThat(tableDescription.getKeySchema().size()).isEqualTo(1);
        KeySchemaElement hashKey = tableDescription.getKeySchema().get(0);
        assertThat(hashKey.getKeyType()).isEqualTo(KeyType.HASH.name());

        assertThat(tableDescription.getAttributeDefinitions().size()).isEqualTo(1);
        AttributeDefinition attributeDefinition = tableDescription.getAttributeDefinitions().get(0);
        assertThat(attributeDefinition.getAttributeName()).isEqualTo(request.getAttributeDefinitions().get(0).getAttributeName());
        assertThat(attributeDefinition.getAttributeType()).isEqualTo(request.getAttributeDefinitions().get(0).getAttributeType());
    }

    @Test
    public void shouldCreateTableWithHashAndRangeKey() throws Exception {
        CreateTableRequest request = CreateTableRequestTestBuilder.valid().withHashAndRangeKey().build();
        Map<String, CreateTableRequest> mapping = Maps.newHashMap();
        mapping.put(request.getTableName(), request);
        differencingEngine.synchronizeLocalAgainstRemote(mapping);

        DescribeTableResult describeTableResult = amazonDynamoDB.describeTable(request.getTableName());
        TableDescription tableDescription = describeTableResult.getTable();
        assertThat(tableDescription.getTableStatus()).isEqualTo("ACTIVE");
        assertThat(tableDescription.getGlobalSecondaryIndexes()).isNull();
        assertThat(tableDescription.getLocalSecondaryIndexes()).isNull();

        assertThat(tableDescription.getKeySchema().size()).isEqualTo(2);
        assertThat(tableDescription.getAttributeDefinitions().size()).isEqualTo(2);
        assertThat(tableDescription.getAttributeDefinitions()).containsAll(request.getAttributeDefinitions());
        assertThat(tableDescription.getKeySchema()).containsAll(request.getKeySchema());

        KeySchemaElement hashKey = null;
        KeySchemaElement rangeKey = null;

        for(KeySchemaElement element: tableDescription.getKeySchema()) {
            if (element.getKeyType().equals(KeyType.HASH.name())) {
                hashKey = element;
            }

            if (element.getKeyType().equals(KeyType.RANGE.name())) {
                rangeKey = element;
            }
        }

        assertThat(hashKey).isNotNull();
        assertThat(rangeKey).isNotNull();
    }

    @Test
    public void shouldCreateTableWithHashAndRangeAndManySimpleGlobalSecondaryIndexes() throws Exception {
        int randomIndexAmount = RandomUtils.nextInt(0, 2) == 0 ? 3 : 5;
        CreateTableRequest request = CreateTableRequestTestBuilder.valid().withHashAndRangeKey().withGlobalSecondaryIndexHashOnlyCount(randomIndexAmount).build();
        Map<String, CreateTableRequest> mapping = Maps.newHashMap();
        mapping.put(request.getTableName(), request);

        Map<String, GlobalSecondaryIndex> indexMapping = Maps.newHashMap();
        for (GlobalSecondaryIndex currentGsi: request.getGlobalSecondaryIndexes()) {
            indexMapping.put(currentGsi.getIndexName(), currentGsi);
        }

        differencingEngine.synchronizeLocalAgainstRemote(mapping);

        DescribeTableResult describeTableResult = amazonDynamoDB.describeTable(request.getTableName());
        TableDescription tableDescription = describeTableResult.getTable();
        assertThat(tableDescription.getTableStatus()).isEqualTo("ACTIVE");
        assertThat(tableDescription.getGlobalSecondaryIndexes()).isNotNull();
        assertThat(tableDescription.getGlobalSecondaryIndexes().size()).isEqualTo(randomIndexAmount);
        assertThat(tableDescription.getLocalSecondaryIndexes()).isNull();
        assertThat(tableDescription.getKeySchema().size()).isEqualTo(2);

        KeySchemaElement hashKey = null;
        KeySchemaElement rangeKey = null;

        for(KeySchemaElement element: tableDescription.getKeySchema()) {
            if (element.getKeyType().equals(KeyType.HASH.name())) {
                hashKey = element;
            }

            if (element.getKeyType().equals(KeyType.RANGE.name())) {
                rangeKey = element;
            }
        }

        assertThat(hashKey).isNotNull();
        assertThat(rangeKey).isNotNull();

        for (GlobalSecondaryIndexDescription currentGsi: tableDescription.getGlobalSecondaryIndexes()) {
            assertThat(indexMapping.containsKey(currentGsi.getIndexName()));
            assertThat(currentGsi.getIndexName().split("-").length).isEqualTo(2);

            List<KeySchemaElement> createdKeySchema = indexMapping.get(currentGsi.getIndexName()).getKeySchema();
            List<KeySchemaElement> queriedKeySchema = currentGsi.getKeySchema();

            for (KeySchemaElement currentElement: createdKeySchema) {
                assertThat(queriedKeySchema).contains(currentElement);
            }
        }
    }

    @Test
    public void shouldCreateTableWithHashAndRangeAndManyComplexGlobalSecondaryIndexes() throws Exception {
        int randomIndexAmount = RandomUtils.nextInt(0, 2) == 0 ? 3 : 5;
        CreateTableRequest request = CreateTableRequestTestBuilder.valid().withHashAndRangeKey().withGlobalSecondaryIndexHashAndRangeCount(randomIndexAmount).build();
        Map<String, CreateTableRequest> mapping = Maps.newHashMap();
        mapping.put(request.getTableName(), request);

        Map<String, GlobalSecondaryIndex> indexMapping = Maps.newHashMap();
        for (GlobalSecondaryIndex currentGsi: request.getGlobalSecondaryIndexes()) {
            indexMapping.put(currentGsi.getIndexName(), currentGsi);
        }

        differencingEngine.synchronizeLocalAgainstRemote(mapping);

        DescribeTableResult describeTableResult = amazonDynamoDB.describeTable(request.getTableName());
        TableDescription tableDescription = describeTableResult.getTable();

        assertThat(tableDescription.getTableStatus()).isEqualTo("ACTIVE");
        assertThat(tableDescription.getGlobalSecondaryIndexes()).isNotNull();
        assertThat(tableDescription.getGlobalSecondaryIndexes().size()).isEqualTo(randomIndexAmount);
        assertThat(tableDescription.getLocalSecondaryIndexes()).isNull();
        assertThat(tableDescription.getKeySchema().size()).isEqualTo(2);

        KeySchemaElement localHashKey = null;
        KeySchemaElement localRangeKey = null;

        for (KeySchemaElement element: tableDescription.getKeySchema()) {
            if (element.getKeyType().equals(KeyType.HASH.name())) {
                localHashKey = element;
            }

            if (element.getKeyType().equals(KeyType.RANGE.name())) {
                localRangeKey = element;
            }
        }

        assertThat(localHashKey).isNotNull();
        assertThat(localRangeKey).isNotNull();
        //noinspection ConstantConditions
        assertThat(localHashKey.getAttributeName()).isNotEqualTo(localRangeKey.getAttributeName());

        for (GlobalSecondaryIndexDescription remoteGsi: tableDescription.getGlobalSecondaryIndexes()) {
            String indexName = remoteGsi.getIndexName();
            GlobalSecondaryIndex localGsi = indexMapping.get(indexName);

            KeySchemaElement localGsiHashKey = null;
            KeySchemaElement remoteGsiHashKey = null;

            KeySchemaElement localGsiRangeKey = null;
            KeySchemaElement remoteGsiRangeKey = null;

            for (KeySchemaElement currentElement: localGsi.getKeySchema()) {
                if (currentElement.getKeyType().equals(KeyType.HASH.name())) {
                    localGsiHashKey = currentElement;
                }

                if (currentElement.getKeyType().equals(KeyType.RANGE.name())) {
                    localGsiRangeKey = currentElement;
                }
            }

            for (KeySchemaElement currentElement: remoteGsi.getKeySchema()) {
                if (currentElement.getKeyType().equals(KeyType.HASH.name())) {
                    remoteGsiHashKey = currentElement;
                }

                if (currentElement.getKeyType().equals(KeyType.RANGE.name())) {
                    remoteGsiRangeKey = currentElement;
                }
            }

            assertThat(localGsiHashKey).isNotNull();
            assertThat(remoteGsiHashKey).isNotNull();
            assertThat(localGsiRangeKey).isNotNull();
            assertThat(remoteGsiRangeKey).isNotNull();

            assertReflectionEquals(localGsiHashKey, remoteGsiHashKey);
            assertReflectionEquals(localGsiRangeKey, remoteGsiRangeKey);

            String[] localIndexNameParts = localGsi.getIndexName().split("-");
            String[] remoteIndexNameParts = remoteGsi.getIndexName().split("-");

            assertThat(localIndexNameParts.length).isEqualTo(3);
            assertThat(remoteIndexNameParts.length).isEqualTo(3);

            assertThat(localIndexNameParts.length).isEqualTo(remoteIndexNameParts.length);
        }
    }

    @Test
    public void shouldDeleteExistingIndexesMultipleTimesForTheSameTable() throws Exception {
        int indexAmount = 3;
        CreateTableRequest request1 = CreateTableRequestTestBuilder.valid().withHashAndRangeKey().withGlobalSecondaryIndexHashAndRangeCount(indexAmount).build();
        CreateTableRequest request2 = CreateTableRequestTestBuilder.valid().withTableName(request1.getTableName()).withKeySchema(request1.getKeySchema()).withGlobalSecondaryIndexHashAndRangeCount(indexAmount).build();
        CreateTableRequest request3 = CreateTableRequestTestBuilder.valid().withTableName(request1.getTableName()).withKeySchema(request1.getKeySchema()).withGlobalSecondaryIndexHashAndRangeCount(indexAmount).build();

        Map<String, GlobalSecondaryIndex> initialGsiMapping = Maps.newHashMap();
        Map<String, GlobalSecondaryIndex> finalGsiMapping = Maps.newHashMap();

        for (GlobalSecondaryIndex currentGsi: request1.getGlobalSecondaryIndexes()) {
            initialGsiMapping.put(currentGsi.getIndexName(), currentGsi);
        }

        for (GlobalSecondaryIndex currentGsi: request3.getGlobalSecondaryIndexes()) {
            finalGsiMapping.put(currentGsi.getIndexName(), currentGsi);
        }

        Map<String, CreateTableRequest> mapping1 = Maps.newHashMap();
        Map<String, CreateTableRequest> mapping2 = Maps.newHashMap();
        Map<String, CreateTableRequest> mapping3 = Maps.newHashMap();

        mapping1.put(request1.getTableName(), request1);
        mapping2.put(request2.getTableName(), request2);
        mapping3.put(request3.getTableName(), request3);

        differencingEngine.synchronizeLocalAgainstRemote(mapping1);
        differencingEngine.synchronizeLocalAgainstRemote(mapping2);
        differencingEngine.synchronizeLocalAgainstRemote(mapping3);

        DescribeTableResult describeTableResult = amazonDynamoDB.describeTable(request1.getTableName());
        TableDescription tableDescription = describeTableResult.getTable();

        assertThat(tableDescription.getTableStatus()).isEqualTo("ACTIVE");
        assertThat(tableDescription.getGlobalSecondaryIndexes()).isNotNull();
        assertThat(tableDescription.getGlobalSecondaryIndexes().size()).isEqualTo(indexAmount);
        assertThat(tableDescription.getLocalSecondaryIndexes()).isNull();
        assertThat(tableDescription.getKeySchema().size()).isEqualTo(2);

        KeySchemaElement localIndexHashKey = null;
        KeySchemaElement localIndexRangeKey = null;

        for (KeySchemaElement element: request1.getKeySchema()) {
            if (element.getKeyType().equals(KeyType.HASH.name())) {
                localIndexHashKey = element;
            }

            if (element.getKeyType().equals(KeyType.RANGE.name())) {
                localIndexRangeKey = element;
            }
        }

        KeySchemaElement remoteIndexHashKey = null;
        KeySchemaElement remoteIndexRangeKey = null;

        for (KeySchemaElement element: tableDescription.getKeySchema()) {
            if (element.getKeyType().equals(KeyType.HASH.name())) {
                remoteIndexHashKey = element;
            }

            if (element.getKeyType().equals(KeyType.RANGE.name())) {
                remoteIndexRangeKey = element;
            }
        }


        assertThat(localIndexHashKey).isNotNull();
        assertThat(localIndexRangeKey).isNotNull();
        assertThat(remoteIndexHashKey).isNotNull();
        assertThat(remoteIndexRangeKey).isNotNull();

        assertReflectionEquals(localIndexHashKey, remoteIndexHashKey);
        assertReflectionEquals(localIndexRangeKey, remoteIndexRangeKey);

        //noinspection ConstantConditions
        assertThat(localIndexHashKey.getAttributeName()).isNotEqualTo(localIndexRangeKey.getAttributeName());
        //noinspection ConstantConditions
        assertThat(remoteIndexHashKey.getAttributeName()).isNotEqualTo(remoteIndexRangeKey.getAttributeName());

        for (GlobalSecondaryIndexDescription remoteGsi: tableDescription.getGlobalSecondaryIndexes()) {
            String indexName = remoteGsi.getIndexName();
            GlobalSecondaryIndex localGsi = finalGsiMapping.get(indexName);

            assertThat(initialGsiMapping.containsKey(indexName)).isFalse();
            assertThat(localGsi).isNotNull();

            KeySchemaElement localGsiHashKey = null;
            KeySchemaElement remoteGsiHashKey = null;

            KeySchemaElement localGsiRangeKey = null;
            KeySchemaElement remoteGsiRangeKey = null;

            for (KeySchemaElement currentElement: localGsi.getKeySchema()) {
                if (currentElement.getKeyType().equals(KeyType.HASH.name())) {
                    localGsiHashKey = currentElement;
                }

                if (currentElement.getKeyType().equals(KeyType.RANGE.name())) {
                    localGsiRangeKey = currentElement;
                }
            }

            for (KeySchemaElement currentElement: remoteGsi.getKeySchema()) {
                if (currentElement.getKeyType().equals(KeyType.HASH.name())) {
                    remoteGsiHashKey = currentElement;
                }

                if (currentElement.getKeyType().equals(KeyType.RANGE.name())) {
                    remoteGsiRangeKey = currentElement;
                }
            }

            assertThat(localGsiHashKey).isNotNull();
            assertThat(remoteGsiHashKey).isNotNull();
            assertThat(localGsiRangeKey).isNotNull();
            assertThat(remoteGsiRangeKey).isNotNull();

            assertReflectionEquals(localGsiHashKey, remoteGsiHashKey);
            assertReflectionEquals(localGsiRangeKey, remoteGsiRangeKey);

            String[] localIndexNameParts = localGsi.getIndexName().split("-");
            String[] remoteIndexNameParts = remoteGsi.getIndexName().split("-");

            assertThat(localIndexNameParts.length).isEqualTo(3);
            assertThat(remoteIndexNameParts.length).isEqualTo(3);

            assertThat(localIndexNameParts.length).isEqualTo(remoteIndexNameParts.length);
        }
    }

    @Test
    public void shouldCreateTableWithMultipleLocalSecondaryIndexesAndAddComplexGlobalSecondaryIndexesWithoutRemovingTheOriginalIndexes() throws Exception {
        int localSecondaryCount = RandomUtils.nextInt(2, 6);
        int globalSecondaryCount = 2;
        CreateTableRequest request1 = CreateTableRequestTestBuilder.valid().withHashAndRangeKey().withLocalSecondaryIndexCount(localSecondaryCount).build();

        Map<String, CreateTableRequest> mapping = Maps.newHashMap();
        mapping.put(request1.getTableName(), request1);

        differencingEngine.synchronizeLocalAgainstRemote(mapping);

        TableDescription description1 = amazonDynamoDB.describeTable(request1.getTableName()).getTable();

        assertThat(description1.getLocalSecondaryIndexes().size()).isEqualTo(localSecondaryCount);
        assertThat(description1.getKeySchema().size()).isEqualTo(2);
        assertThat(description1.getTableStatus()).isEqualTo(TableStatus.ACTIVE.name());

        CreateTableRequest request2 = CreateTableRequestTestBuilder.valid().withTableName(request1.getTableName()).withGlobalSecondaryIndexHashAndRangeCount(globalSecondaryCount).build();
        mapping.put(request2.getTableName(), request2); //override the original entry because the table names are identical

        differencingEngine.synchronizeLocalAgainstRemote(mapping);

        TableDescription description2 = amazonDynamoDB.describeTable(request2.getTableName()).getTable();

        assertThat(description2.getLocalSecondaryIndexes().size()).isEqualTo(localSecondaryCount);
        assertThat(description2.getKeySchema().size()).isEqualTo(2);
        assertThat(description2.getTableStatus()).isEqualTo(TableStatus.ACTIVE.name());
        assertThat(description2.getGlobalSecondaryIndexes().size()).isEqualTo(globalSecondaryCount);

        CreateTableRequest request3 = CreateTableRequestTestBuilder.valid().withTableName(request1.getTableName()).withGlobalSecondaryIndices(request2.getGlobalSecondaryIndexes()).withGlobalSecondaryIndexHashAndRangeCount(globalSecondaryCount).build();
        mapping.put(request3.getTableName(), request3); //override the original entry because the table names are identical

        differencingEngine.synchronizeLocalAgainstRemote(mapping);

        TableDescription description3 = amazonDynamoDB.describeTable(request3.getTableName()).getTable();

        assertThat(description3.getLocalSecondaryIndexes().size()).isEqualTo(localSecondaryCount);
        assertThat(description3.getKeySchema().size()).isEqualTo(2);
        assertThat(description3.getTableStatus()).isEqualTo(TableStatus.ACTIVE.name());
        assertThat(description3.getGlobalSecondaryIndexes().size()).isEqualTo(globalSecondaryCount * 2);

        List<GlobalSecondaryIndex> newGlobalSecondaryIndexList = Lists.newLinkedList();
        GlobalSecondaryIndex shouldGetDeleted = null;
        int randomToDelete = RandomUtils.nextInt(0, description3.getGlobalSecondaryIndexes().size());
        for (int i = 0; i < description3.getGlobalSecondaryIndexes().size(); i++) {
            if (i == randomToDelete) {
                shouldGetDeleted = request3.getGlobalSecondaryIndexes().get(i);
            } else {
                newGlobalSecondaryIndexList.add(request3.getGlobalSecondaryIndexes().get(i));
            }
        }

        CreateTableRequest request4 = CreateTableRequestTestBuilder.valid().withTableName(request2.getTableName()).withGlobalSecondaryIndices(newGlobalSecondaryIndexList).withGlobalSecondaryIndexHashAndRangeCount(globalSecondaryCount).build();
        mapping.put(request4.getTableName(), request4); //override the original entry because the table names are identical
        assertThat(request4.getGlobalSecondaryIndexes().size()).isEqualTo(5);

        differencingEngine.synchronizeLocalAgainstRemote(mapping);

        TableDescription description4 = amazonDynamoDB.describeTable(request4.getTableName()).getTable();

        assertThat(description4.getLocalSecondaryIndexes().size()).isEqualTo(localSecondaryCount);
        assertThat(description4.getKeySchema().size()).isEqualTo(2);
        assertThat(description4.getTableStatus()).isEqualTo(TableStatus.ACTIVE.name());
        assertThat(description4.getGlobalSecondaryIndexes().size()).isEqualTo((globalSecondaryCount * 2) + 1);

        // make sure the index we wanted to delete is gone
        for (GlobalSecondaryIndexDescription globalSecondaryIndexDescription: description4.getGlobalSecondaryIndexes()) {
            assertThat(globalSecondaryIndexDescription.getIndexName()).isNotEqualTo(shouldGetDeleted.getIndexName());
        }
    }
}
