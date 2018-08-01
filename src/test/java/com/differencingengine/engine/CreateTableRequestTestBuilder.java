package com.differencingengine.engine;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Collection;

public class CreateTableRequestTestBuilder {

    private String tableName = RandomStringUtils.randomAlphanumeric(10);
    private Collection<KeySchemaElement> keySchema = Lists.newLinkedList();
    private Collection<AttributeDefinition> attributeDefinitions = Lists.newLinkedList();
    private Collection<GlobalSecondaryIndex> globalSecondaryIndices = Lists.newLinkedList();
    private Collection<LocalSecondaryIndex> localSecondaryIndices = Lists.newLinkedList();
    private ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput(1L, 1L);

    public static CreateTableRequestTestBuilder valid() {
        return new CreateTableRequestTestBuilder();
    }

    public CreateTableRequest build() {
        CreateTableRequest request = new CreateTableRequest();

        if (!tableName.startsWith("dev_")) {
            tableName = "dev_" + tableName;
        }

        request.setTableName(tableName);
        request.setKeySchema(keySchema);
        if (attributeDefinitions.size() > 0) {
            request.setAttributeDefinitions(attributeDefinitions);
        }

        if (globalSecondaryIndices.size() > 0) {
            request.setGlobalSecondaryIndexes(globalSecondaryIndices);
        }

        if (localSecondaryIndices.size() > 0) {
            request.setLocalSecondaryIndexes(localSecondaryIndices);
        }

        request.setProvisionedThroughput(provisionedThroughput);

        return request;
    }

    public CreateTableRequestTestBuilder withKeySchema(Collection<KeySchemaElement> keySchema) { this.keySchema = keySchema; return this; }

    public CreateTableRequestTestBuilder withTableName(String tableName) { this.tableName = tableName; return this; }

    public CreateTableRequestTestBuilder withAttributeDefinitions(Collection<AttributeDefinition> attributeDefinitions) { this.attributeDefinitions = attributeDefinitions; return this; }

    public CreateTableRequestTestBuilder withGlobalSecondaryIndices(Collection<GlobalSecondaryIndex> globalSecondaryIndices) { this.globalSecondaryIndices = globalSecondaryIndices; return this; }

    public CreateTableRequestTestBuilder withLocalSecondaryIndices(Collection<LocalSecondaryIndex> localSecondaryIndices) { this.localSecondaryIndices = localSecondaryIndices; return this; }

    public CreateTableRequestTestBuilder withProvisionedThroughput(ProvisionedThroughput provisionedThroughput) { this.provisionedThroughput = provisionedThroughput; return this; }

    public CreateTableRequestTestBuilder withHashKeyOnly() {
        String attributeName = RandomStringUtils.randomAlphanumeric(10);

        keySchema.add(
                new KeySchemaElement()
                    .withAttributeName(attributeName)
                    .withKeyType(KeyType.HASH));

        attributeDefinitions.add(
                new AttributeDefinition()
                    .withAttributeName(attributeName)
                    .withAttributeType(ScalarAttributeType.S));

        return this;
    }

    public CreateTableRequestTestBuilder withHashAndRangeKey() {
        String hashKeyAttributeName = RandomStringUtils.randomAlphanumeric(10);
        String rangeKeyAttributeName = RandomStringUtils.randomAlphanumeric(10);

        keySchema.add(
                new KeySchemaElement()
                    .withAttributeName(hashKeyAttributeName)
                    .withKeyType(KeyType.HASH));

        keySchema.add(
                new KeySchemaElement()
                    .withAttributeName(rangeKeyAttributeName)
                    .withKeyType(KeyType.RANGE));

        attributeDefinitions.add(
                new AttributeDefinition()
                    .withAttributeName(hashKeyAttributeName)
                    .withAttributeType(ScalarAttributeType.S));

        attributeDefinitions.add(
                new AttributeDefinition()
                    .withAttributeName(rangeKeyAttributeName)
                    .withAttributeType(ScalarAttributeType.S));

        return this;
    }

    public CreateTableRequestTestBuilder withLocalSecondaryIndexCount(int count) {

        KeySchemaElement hashKey = null;
        for (KeySchemaElement keySchemaElement: this.keySchema) {
            if (keySchemaElement.getKeyType().equals(KeyType.HASH.name())) {
                hashKey = keySchemaElement;
            }
        }

        if (hashKey == null) {
            throw new IllegalArgumentException("Cannot create local secondary index without a hash key definition first.");
        }

        for (int i = 0; i < count; i++) {
            String localSecondaryIndexRangeKeyName = RandomStringUtils.randomAlphanumeric(10);

            localSecondaryIndices.add(
                    new LocalSecondaryIndex()
                        .withIndexName(localSecondaryIndexRangeKeyName + "-Index")
                        .withKeySchema(
                                new KeySchemaElement()
                                    .withAttributeName(hashKey.getAttributeName())
                                    .withKeyType(hashKey.getKeyType()),
                                new KeySchemaElement()
                                    .withAttributeName(localSecondaryIndexRangeKeyName)
                                    .withKeyType(KeyType.RANGE))
                        .withProjection(
                                new Projection()
                                        .withProjectionType(ProjectionType.ALL))
            );

            attributeDefinitions.add(
                    new AttributeDefinition()
                        .withAttributeName(localSecondaryIndexRangeKeyName)
                        .withAttributeType(ScalarAttributeType.S)
            );
        }

        return this;
    }

    public CreateTableRequestTestBuilder withGlobalSecondaryIndexHashOnlyCount(int count) {
        for (int i = 0; i < count; i++) {
            String hashIndexName = RandomStringUtils.randomAlphanumeric(10);

            globalSecondaryIndices.add(
                    new GlobalSecondaryIndex()
                        .withIndexName(hashIndexName + "-Index")
                        .withKeySchema(
                                new KeySchemaElement()
                                    .withAttributeName(hashIndexName)
                                    .withKeyType(KeyType.HASH))
                        .withProjection(
                                new Projection()
                                        .withProjectionType(ProjectionType.KEYS_ONLY))
                        .withProvisionedThroughput(
                                new ProvisionedThroughput(1L, 1L)));

            attributeDefinitions.add(
                    new AttributeDefinition()
                        .withAttributeName(hashIndexName)
                        .withAttributeType(ScalarAttributeType.S));
        }

        return this;
    }

    public CreateTableRequestTestBuilder withGlobalSecondaryIndexHashAndRangeCount(int count) {
        for (int i = 0; i < count; i++) {
            String hashIndexName = RandomStringUtils.randomAlphanumeric(10);
            String rangeIndexName = RandomStringUtils.randomAlphanumeric(10);

            globalSecondaryIndices.add(
                    new GlobalSecondaryIndex()
                        .withIndexName(hashIndexName + "-" + rangeIndexName + "-Index")
                        .withKeySchema(Lists.newArrayList(
                            new KeySchemaElement()
                                    .withAttributeName(hashIndexName)
                                    .withKeyType(KeyType.HASH),
                            new KeySchemaElement()
                                    .withAttributeName(rangeIndexName)
                                    .withKeyType(KeyType.RANGE)))
                    .withProjection(
                            new Projection()
                                    .withProjectionType(ProjectionType.KEYS_ONLY))
                    .withProvisionedThroughput(
                            new ProvisionedThroughput(1L, 1L)));

            attributeDefinitions.add(
                    new AttributeDefinition()
                        .withAttributeName(hashIndexName)
                        .withAttributeType(ScalarAttributeType.S));

            attributeDefinitions.add(
                    new AttributeDefinition()
                        .withAttributeName(rangeIndexName)
                        .withAttributeType(ScalarAttributeType.S));
        }

        return this;
    }
}
