package com.github.enemes2000.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.HashMap;
import java.util.Map;

public abstract class ReplaceSymbol<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Replace the source symbol with the destination  symbol in the table name and the field name";

    private final Map<String, String> mapOfoldFieldAndNewField = new HashMap<>();

    private interface ConfigName {
        String SOURCE_SYMBOL_VALUE = "source.symbol.value";
        String DESTINATION_SYMBOL_VALUE="destination.symbol.value";
    }


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_SYMBOL_VALUE, ConfigDef.Type.STRING, "_", ConfigDef.Importance.HIGH,
                    "Source symbol value")
            .define(ConfigName.DESTINATION_SYMBOL_VALUE,  ConfigDef.Type.STRING, "_", ConfigDef.Importance.HIGH,
                    "Destination symbol value");

    private static final String PURPOSE = "replacing the source symbol with the destination symbol";

    private String sourceSymbolValue;
    private String destinationSymbolvalue;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> map) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
        sourceSymbolValue = config.getString(ConfigName.SOURCE_SYMBOL_VALUE);
        destinationSymbolvalue = config.getString(ConfigName.DESTINATION_SYMBOL_VALUE);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        R newRecord = null;
        String topicName = record.topic();
        String firstOccurence = topicName.split(sourceSymbolValue)[0];
        if(!firstOccurence.isEmpty()){
            String newTopicName = getNewNameFrom(topicName, firstOccurence);
            System.out.println(newTopicName);
            newRecord =  record.newRecord(newTopicName, record.kafkaPartition(), record.keySchema(), record.valueSchema(), record.valueSchema(), record.value(), record.timestamp());
        }
        if (newRecord != null) record = newRecord;

        return applyWithSchema(record);

    }

    private String getNewNameFrom(String topicName, String firstOccurence) {
        final String source = firstOccurence.concat(sourceSymbolValue);
        final String destination = firstOccurence.concat(destinationSymbolvalue);
        final String newTopicName = destination.concat(topicName.substring(source.length()));
        return newTopicName;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (mapOfoldFieldAndNewField.containsKey(field.name()))
                 updatedValue.put(mapOfoldFieldAndNewField.get(field.name()), value.get(field));
            else
                updatedValue.put(field.name(), value.get(field));
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }


    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field: schema.fields()) {
            String fieldName = field.name();
            String arrayString[] = fieldName.split(sourceSymbolValue);

            if (arrayString.length > 1){
                String fieldNameFirstOccurence = arrayString[0];
                String newFieldName = getNewNameFrom(fieldName, fieldNameFirstOccurence);
                mapOfoldFieldAndNewField.put(fieldName, newFieldName);
                builder.field(newFieldName, field.schema());
            }else {
                builder.field(field.name(), field.schema());
            }
        }

        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ReplaceSymbol<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends ReplaceSymbol<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}
