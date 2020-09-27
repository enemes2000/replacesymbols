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

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class ReplaceSymbol<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Replace the source symbol with the destination  symbol in the table name and the field name";

    private final Map<String, String> mapOfoldFieldAndNewField = new HashMap<>();

    private interface ConfigName {
        String SOURCE_SYMBOL_VALUE = "source.symbol.value";
        String DESTINATION_SYMBOL_VALUE="destination.symbol.value";
        String POSITIONS_ON_TABLE_NAME = "symbol.table.positions";
        String POSITIONS_FIELD_NAME = "symbol.field.positions";
        String AFFECTED_FIELD = "source.field.name";
        String AFFECTED_TABLE="source.table.name";
    }


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_SYMBOL_VALUE, ConfigDef.Type.STRING, "_", ConfigDef.Importance.HIGH,
                    "Symbol to be replaced")
            .define(ConfigName.DESTINATION_SYMBOL_VALUE,  ConfigDef.Type.STRING, "_", ConfigDef.Importance.HIGH,
                    "Symbol replacing")
            .define(ConfigName.POSITIONS_ON_TABLE_NAME, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                    "List of all the position where the symbol need to be replaced in the table name")
            .define(ConfigName.POSITIONS_FIELD_NAME, ConfigDef.Type.LIST,ConfigDef.Importance.HIGH,
                    "List of all the position where symbol need to be replaced in the field name")
            .define(ConfigName.AFFECTED_FIELD, ConfigDef.Type.LIST, ConfigDef.Importance.MEDIUM,
                    "List of fields that need the symbol to change")
            .define(ConfigName.AFFECTED_TABLE, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM,
                    "Table name that needs the symbol to change");



    private static final String PURPOSE = "find the source symbol in table name, field name and replace it with the destination symbol";

    private String sourceSymbolValue;
    private String destinationSymbolvalue;
    private List<String> affectedTablePositions;
    private List<String> affectedFieldPositions;
    private String affectedTabe;
    private List<String> affectedFields;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> map) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
        sourceSymbolValue = config.getString(ConfigName.SOURCE_SYMBOL_VALUE);
        destinationSymbolvalue = config.getString(ConfigName.DESTINATION_SYMBOL_VALUE);
        affectedTablePositions = config.getList(ConfigName.POSITIONS_ON_TABLE_NAME);
        affectedFieldPositions = config.getList(ConfigName.POSITIONS_FIELD_NAME);
        affectedTabe = config.getString(ConfigName.AFFECTED_TABLE);
        affectedFields = config.getList(ConfigName.AFFECTED_FIELD);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        R newRecord = null;
        String topicName = record.topic();
        List<Integer> symbolTableNamePositions = getSymbolPositionsInTableName(affectedTablePositions);
        String myString = findAndReplaceSymbolInTableName(topicName, symbolTableNamePositions);
        System.out.println(myString);
        String firstOccurence = topicName.split(sourceSymbolValue)[0];
        if(!firstOccurence.isEmpty()){
            String newTopicName = getNewNameFrom(topicName, firstOccurence);
            System.out.println(newTopicName);
            newRecord =  record.newRecord(newTopicName, record.kafkaPartition(), record.keySchema(), record.valueSchema(), record.valueSchema(), record.value(), record.timestamp());
        }
        if (newRecord != null) record = newRecord;

        return applyWithSchema(record);

    }

    private List<Integer> getSymbolPositionsInTableName(List<String> affectedTablePositions) {
        final int position = 0;
        if (affectedTablePositions.isEmpty()) return new ArrayList<>(position);
        else {
           // List<String> positions = Arrays.asList(affectedTablePositions.split(","));

            return affectedTablePositions.stream()
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());

        }
    }

    private String findAndReplaceSymbolInTableName(String tableName, List<Integer> positions){
        final String [] stringArray = tableName.split(sourceSymbolValue);
        List<String> strings = Pattern.compile(sourceSymbolValue).splitAsStream(tableName)
        .collect(Collectors.toList());
        StringBuffer buffer = new StringBuffer();
        StringBuffer buf = new StringBuffer();
        for(int i = 0; i < strings.size(); i++){
            if (i < positions.size()) {
                for (int j = i; j < positions.size(); j++) {
                    String val = strings.get(i);
                    String symbol = getSymbolFrom(positions, j);
                    buf.append(val).append(symbol);
                    if (i + 1 == positions.get(i)) {
                        System.out.println(symbol);
                        buffer.append(val).append(destinationSymbolvalue);
                    }else {
                        System.out.println(symbol);
                        buffer.append(val).append(sourceSymbolValue);
                    }
                    break;
                }
            }else if(i < strings.size() - 1){
                buffer.append(strings.get(i)).append(sourceSymbolValue);
            }
        }
        if (!tableName.endsWith(sourceSymbolValue)){
            buffer.append(strings.get(strings.size() - 1));
            buf.append(strings.get(strings.size() - 1));
        }
        else {
            buffer.append(strings.get(strings.size() - 1)).append(sourceSymbolValue);
            buf.append(strings.get(strings.size() - 1)).append(sourceSymbolValue);
        }
        System.out.println(buf.toString());
        return buffer.toString();
    }

    private List<String> initializeSymbolList(List<Integer> positions){
        return positions.stream()
                .map(x-> sourceSymbolValue)
                .collect(Collectors.toList());
    }

    private List<String> insertSymbolIn(List<Integer> positions){
        List<String> symbols = initializeSymbolList(positions);
        for(int i = 0; i < symbols.size(); i++){
            if (i < positions.size() && positions.get(i) != 0)
            symbols.add(positions.get(i), destinationSymbolvalue);
        }
        return new ArrayList<>(symbols);
    }
    private String getSymbolFrom(List<Integer> positions,Integer position){
        return insertSymbolIn(positions).get(position);
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

    private R applWithoutSchema(R record){
        return null;
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
