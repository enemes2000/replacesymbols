package com.github.enemes2000.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;
import sun.awt.geom.AreaOp;

import java.util.HashMap;
import java.util.Map;

import static com.github.enemes2000.kafka.connect.smt.ReplaceSymbol.CONFIG_DEF;
import static org.junit.Assert.assertEquals;

public class ReplaceSymbolTest {
    private final ReplaceSymbol<SinkRecord> recordReplaceSymbol = new ReplaceSymbol.Value<>();

    @After
    public void tearDown(){
        recordReplaceSymbol.close();
    }

    @Test
    public void withSchema(){


        final Map<String, String> props = new HashMap<>();
        props.put("source.symbol.value", "_");
        props.put("destination.symbol.value", "$");
        props.put("symbol.table.positions", "1,3");
        props.put("symbol.field.positions", "1,2");
        props.put("source.field.name", "my_field");
        props.put("source.table.name", "my_topic_in_action_");
        recordReplaceSymbol.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("admin_action_plan", Schema.STRING_SCHEMA)
                .field("actionplan", Schema.STRING_SCHEMA)
                .build();
        final Struct value = new Struct(schema);

        value.put("admin_action_plan", "toto");
        value.put("actionplan", "toti");

        final SinkRecord record = new SinkRecord(props.get("source.table.name"), 0, null, null, schema, value,0);

        final SinkRecord transformedRecord = recordReplaceSymbol.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals("toto", updatedValue.get("admin$action_plan"));

        assertEquals("test$mike_topic", transformedRecord.topic());
    }
}
