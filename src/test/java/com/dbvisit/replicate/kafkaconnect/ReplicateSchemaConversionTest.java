package com.dbvisit.replicate.kafkaconnect;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Schema.Type;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.MetaDataRecord;
import com.dbvisit.replicate.plog.metadata.Column;
import com.dbvisit.replicate.plog.metadata.ColumnState;

/**
 *  Test schema conversion only
 */
public class ReplicateSchemaConversionTest extends ReplicateTestConfig {
    private static final Logger logger = LoggerFactory.getLogger (
        ReplicateSchemaConversionTest.class
    );
    
    final private String mockName = "MOCK-SOE.UNITTEST";
    final private ConnectorMode defaultMode = new ConnectorMode (
        ConnectorCDCFormat.CHANGEROW,
        true,
        true,
        false
    );
    
    /* test different modes of publishing */
    @Test
    public void testCreateAndUpdateSchema() {
        /* default: changerow, txmeta */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGEROW,
                true,
                false,
                false
            )
        );
        /* changerow, txmeta, keys */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGEROW,
                true,
                true,
                false
            )
        );
        /* changerow, txmeta, keys, no schema evolution */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGEROW,
                true,
                true,
                true
            )
        );
        /* changerow, no txmeta, no keys */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGEROW,
                false,
                false,
                false
            )
        );
        /* changerow, no txmeta, keys */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGEROW,
                false,
                true,
                false
            )
        );
        /* changerow, no txmeta, keys, no schema evolution */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGEROW,
                false,
                true,
                true
            )
        );
        /* changerow, no txmeta, no keys, no schema evolution */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGEROW,
                false,
                false,
                true
            )
        );
        /* changerow, txmeta, no keys, no schema evolution */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGEROW,
                true,
                false,
                true
            )
        );
        
        /* changeset, txmeta */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGESET,
                true,
                false,
                false
            )
        );
        /* changeset, txmeta, keys */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGESET,
                true,
                true,
                false
            )
        );
        /* changeset, txmeta, keys, no schema evolution */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGESET,
                true,
                true,
                true
            )
        );
        /* changeset, no txmeta, no keys */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGESET,
                false,
                false,
                false
            )
        );
        /* changeset, no txmeta, keys */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGESET,
                false,
                true,
                false
            )
        );
        /* changeset, no txmeta, no keys, no schema evolution */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGESET,
                false,
                false,
                true
            )
        );
        /* changeset, no txmeta, no keys, no schema evolution */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGESET,
                false,
                false,
                true
            )
        );
        /* chanegset, txmeta, no keys, no schema evolution */
        testCreateAndUpdateSchema (
            new ConnectorMode (
                ConnectorCDCFormat.CHANGESET,
                true,
                false,
                true
            )
        );
    }

    @SuppressWarnings("serial")
    public void testCreateAndUpdateSchema(final ConnectorMode mode) {
        try {
            final String mockName = "MOCK-SOE.UNITTEST"; 
            
            /* create and verify first version of schema */
            MetaDataRecord mdr1 =
                MetaDataRecord.fromJSONString(METADATA_RECORD_1_JSON);
            
            logger.info (mdr1.toJSONString());
            
            TopicSchema topicSchema1 =
                new ReplicateSchemaConverter()
                    .topicName(mockName)
                    .metadata(mdr1.getMetaData())
                    .mode(mode)
                    .convert();
            
            Schema schema1 = topicSchema1.valueSchema();
            
            logger.info ("In: " + mdr1.toJSONString());
            logger.info (
                "Out: schema name=" + schema1.name() + " " +
                "type=" + schema1.type()
            );
            
            /* only used by change sets */
            final Schema keySchema = Schema.STRING_SCHEMA;
            final Schema valueSchema = SchemaBuilder.struct()
                .name("CHANGE_SET")
                .field (
                    "ID", Schema.OPTIONAL_INT32_SCHEMA
                )
                .field (
                    "TEST_NAME", Schema.OPTIONAL_STRING_SCHEMA
                ).build();
            
            validateSchema (
                mdr1, 
                schema1,
                mode.getCDCFormat().equals (ConnectorCDCFormat.CHANGEROW) 
                ?
                    new LinkedHashMap<String, Schema>() {{
                        /* add TXMETA field is enabled */
                        if (mode.publishTxInfo()) {
                            put ("XID", Schema.STRING_SCHEMA);
                            put ("TYPE", Schema.STRING_SCHEMA);
                            put ("CHANGE_ID", Schema.INT64_SCHEMA);
                        }
                        put ("ID", Schema.INT32_SCHEMA);
                        put ("TEST_NAME", Schema.OPTIONAL_STRING_SCHEMA);
                    }}   
                :
                    new LinkedHashMap<String, Schema>() {{
                        /* add TXMETA field is enabled */
                        if (mode.publishTxInfo()) {
                            put ("XID", Schema.STRING_SCHEMA);
                            put ("TYPE", Schema.STRING_SCHEMA);
                            put ("CHANGE_ID", Schema.INT64_SCHEMA);
                        }
                        put (
                            "CHANGE_DATA",
                            SchemaBuilder.map (keySchema, valueSchema)
                        );
                    }}   
            );
            
            /* next create v2 of schema using v1 above and verify */
            MetaDataRecord mdr2 =
                MetaDataRecord.fromJSONString(METADATA_RECORD_2_JSON);
            
            for (int i = 0; i < 3; i++) {
                Column col = mdr2.getMetaData().getTableColumns().get(i);
                if (i < 2) {
                    /* existing column - set these columns as unchanged */
                    col.setState(ColumnState.UNCHANGED);
                }
                else {
                    /* new column */
                    col.setState(ColumnState.ADDED);
                }
            }
            
            TopicSchema topicSchema2 =
                new ReplicateSchemaConverter()
                    .topicName(mockName)
                    .schema(topicSchema1)
                    .metadata(mdr2.getMetaData())
                    .mode(mode)
                    .update();

            Schema schema2 = topicSchema2.valueSchema();
            
            logger.info ("In: " + mdr2.toJSONString());
            logger.info (
                "Out: schema name=" + schema2.name() + " " +
                "type=" + schema2.type()
            );
            
            Schema updatedSchema = null;
            
            if (!mode.noSchemaEvolution()) {
                updatedSchema = SchemaBuilder.struct()
                    .name("CHANGE_SET")
                    .field (
                        "ID", Schema.OPTIONAL_INT32_SCHEMA
                    )
                    .field (
                        "TEST_NAME", Schema.OPTIONAL_STRING_SCHEMA
                    )
                    .field (
                        "SCORE", 
                        Decimal.builder(4)
                               .defaultValue(BigDecimal.ZERO.setScale(4))
                               .build()
                    ).build();
            }
            else {
                /* schema stays the same */
                updatedSchema = valueSchema;
            }
            
            final Schema newValueSchema = updatedSchema;

            validateSchema (
                mdr2,
                schema2,
                mode.getCDCFormat().equals (ConnectorCDCFormat.CHANGEROW) 
                ?
                    new LinkedHashMap<String, Schema>() {{
                        /* add TXMETA field is enabled */
                        if (mode.publishTxInfo()) {
                            put ("XID", Schema.STRING_SCHEMA);
                            put ("TYPE", Schema.STRING_SCHEMA);
                            put ("CHANGE_ID", Schema.INT64_SCHEMA);
                        }
                        put ("ID", Schema.INT32_SCHEMA);
                        put ("TEST_NAME", Schema.OPTIONAL_STRING_SCHEMA);
                        /* add new field if schema evolution is enabled */
                        if (!mode.noSchemaEvolution()) {
                            /* decimal encoded as BYTES, in DDL it's mandatory field,
                             * but in schema it has to have default value and be
                             * nullable */
                            put (
                                "SCORE", 
                                Decimal.builder(4)
                                       .defaultValue(BigDecimal.ZERO.setScale(4))
                                       .build()
                            );
                        }
                    }}
                :
                    new LinkedHashMap<String, Schema>() {{
                        /* add TXMETA field is enabled */
                        if (mode.publishTxInfo()) {
                            put ("XID", Schema.STRING_SCHEMA);
                            put ("TYPE", Schema.STRING_SCHEMA);
                            put ("CHANGE_ID", Schema.INT64_SCHEMA);
                        }
                        put (
                            "CHANGE_DATA",
                            SchemaBuilder.map (keySchema, newValueSchema)
                        );
                    }}   
                        
            );
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
    public void validateSchema (
        final MetaDataRecord mdr,
        final Schema schema,
        final LinkedHashMap<String, Schema> fieldSchemas
    ) {
        assertTrue (
            "Expecting schema: " + mockName + ", got: " + schema.name(),
            schema.name().equals (mockName)
        );
        
        assertTrue (
            "Expecting schema of type STRUCT, got: " + schema.type(),
            schema.type().equals (Type.STRUCT)
        );
        
        final List<Column> expectedColumns = 
            mdr.getMetaData().getTableColumns();
        
        /* expect same as defined for field schemas */
        int expectedNumFields = fieldSchemas.size();
        
        assertTrue (
            "Expecting " + expectedNumFields + ", got: " +
            schema.fields().size(),
            schema.fields().size() == expectedNumFields    
        );
        
        /* extra in all of the payload and must always be present */
        List <String> expectedFields = 
            new ArrayList<String> (fieldSchemas.keySet());
        
        for (int i = 0; i < expectedFields.size(); i++) { 
            String expectedField = expectedFields.get (i);
            
            logger.info (
                "Checking field: " + expectedField + " " + 
                "index: " + i + " " + 
                "type: " + fieldSchemas.get(expectedField).type()
            );
            
            assertTrue (
                "Expecting field: " + expectedField + ", was not " +
                "converted to kafka schema field",
                schema.field(expectedField) != null 
            );
            
            assertTrue (
                "Expecting field: " + expectedField + " at index: " + i +
                ", found it at index: " + 
                schema.field(expectedField).index(),
                schema.field(expectedField).index() == i  
            );
            
            Field field = schema.field (expectedField);
            Schema expectedSchema = fieldSchemas.get(expectedField);
            
            /* extra fields if TX META is enabled */
            int extraFields = fieldSchemas.size() - expectedColumns.size();
            
            assertTrue (
                "Expecting field: " + expectedField + " with schema type=" +
                expectedSchema.type() + " optional=" + 
                expectedSchema.isOptional() + " default: " + 
                (expectedSchema.defaultValue() == null 
                    ? "N/A"
                    : expectedSchema.defaultValue()
                ) +
                ", got: " +
                field.schema().type() + " optional=" + 
                field.schema().isOptional() + " default: " + 
                (field.schema().defaultValue() == null 
                    ? "N/A"
                    : field.schema().defaultValue()
                ),
                field.schema().type().equals(
                    fieldSchemas.get(expectedField).type()
                ) &&
                field.schema().isOptional() == 
                fieldSchemas.get(expectedField).isOptional()
                &&
                (
                    (
                        field.schema().defaultValue() != null &&
                        (
                            !field.schema().type().equals (Type.BYTES) ||
                            (
                                i >= extraFields &&
                                expectedColumns.get(i - extraFields).getType()
                                    .equals("NUMBER")
                            )
                        )
                        &&
                        field.schema().defaultValue().equals(
                            fieldSchemas.get(expectedField).defaultValue()
                        )
                    ) ||
                    (
                        field.schema().defaultValue() != null &&
                        (
                            field.schema().type().equals (Type.BYTES) &&
                            i >= extraFields &&
                            !expectedColumns.get(i - extraFields).getType()
                                .equals("NUMBER")
                        ) &&
                        ((byte [])field.schema().defaultValue()).length == 0
                    ) ||
                    (
                        field.schema().defaultValue() == null &&
                        field.schema().defaultValue() == 
                        fieldSchemas.get(expectedField).defaultValue()
                    )
                )
            );
        }
    }
    
    /* testing raw types, as in the ones we support from Oracle
     * 
        NUMBER int32 int64 decimal
        VARCHAR2 String
        VARCHAR String
        CHAR String
        NVARCHAR2 String
        NVARCHAR String
        NCHAR String
        LONG String
        INTERVAL DAY TO SECOND String
        INTERVAL YEAR TO MONTH String
        CLOB String
        NCLOB String
        DATE Timestamp
        TIMESTAMP Timestamp
        TIMESTAMP WITH TIME ZONE Timestamp
        TIMESTAMP WITH LOCAL TIME ZONE Timestamp
        BLOB Bytes
        RAW Bytes
        LONG RAW Bytes
     */
    
    @Test
    public void testConvertNumberInt32 () {
        testConvertSchema (
            new Column(0, "TEST", "NUMBER", 6, 0, false),
            Schema.INT32_SCHEMA
        );
    }
    
    @Test
    public void testConvertNumberInt32Nullable () {
        testConvertSchema (
            new Column(0, "TEST", "NUMBER", 6, 0, true),
            Schema.OPTIONAL_INT32_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateNumberInt32  () {
        /* force in NOT-NULL, expect DEFAULT VALUE 0 and NOT-NULL for INT32 */
        testUpdateSchema (
            new Column(0, "TEST", "NUMBER", 6, 0, false),
            SchemaBuilder.int32().defaultValue(0).build()
        );      
    }
    
    @Test
    public void testConvertNumberInt64 () {
        testConvertSchema (
            new Column(0, "TEST", "NUMBER", 18, 0, false),
            Schema.INT64_SCHEMA
        );
    }
    
    @Test
    public void testConvertNumberInt64Nullable () {
        testConvertSchema (
            new Column(0, "TEST", "NUMBER", 18, 0, true),
            Schema.OPTIONAL_INT64_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateNumberInt64  () {
        /* force in NOT-NULL, expect DEFAULT VALUE O and NOT-NULL for INT64 */
        testUpdateSchema (
            new Column(0, "TEST", "NUMBER", 18, 0, false),
            SchemaBuilder.int64().defaultValue(0L).build()
        );      
    }
    
    @Test
    public void testConvertNumber () {
        testConvertSchema (
            new Column(0, "TEST", "NUMBER", 28, 10, false),
            Decimal.schema(10)
        );
    }
    
    @Test
    public void testConvertNumberNullable () {
        testConvertSchema (
            new Column(0, "TEST", "NUMBER", 28, 10, true),
            Decimal.builder(10).optional().build()
        );
    }
    
    @Test 
    public void testUpdateNumber () {
        /* force in NOT-NULL, expect DEFAULT VALUE 0 and NOT-NULL for INT64 */
        testUpdateSchema (
            new Column(0, "TEST", "NUMBER", 28, 10, false),
            Decimal.builder(10)
                   .defaultValue(new BigDecimal(0).setScale(10))
                   .build()
        );      
    }
    
    
    @Test
    public void testConvertVarchar2 () {
        testConvertSchema (
            new Column(0, "TEST", "VARCHAR2", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertVarchar2Nullable () {
        testConvertSchema (
            new Column(0, "TEST", "VARCHAR2", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateVarchar2 () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "VARCHAR2", -1, -1, false),
            SchemaBuilder.string().defaultValue("").build()
        );      
    }
    
    
    @Test
    public void testConvertVarchar () {
        testConvertSchema (
            new Column(0, "TEST", "VARCHAR", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertVarcharNullable () {
        testConvertSchema (
            new Column(0, "TEST", "VARCHAR", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateVarchar () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "VARCHAR", -1, -1, false),
            SchemaBuilder.string().defaultValue("").build()
        );      
    }
  
    @Test
    public void testConvertChar () {
        testConvertSchema (
            new Column(0, "TEST", "CHAR", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertCharNullable () {
        testConvertSchema (
            new Column(0, "TEST", "CHAR", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateChar () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "CHAR", -1, -1, false),
            SchemaBuilder.string().defaultValue("").build()
        );      
    }
  
    @Test
    public void testConvertNvarchar2 () {
        testConvertSchema (
            new Column(0, "TEST", "NVARCHAR2", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertNvarchar2Nullable () {
        testConvertSchema (
            new Column(0, "TEST", "NVARCHAR2", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateNvarchar2 () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "NVARCHAR2", -1, -1, false),
            SchemaBuilder.string().defaultValue("").build()
        );      
    }
  
    @Test
    public void testConvertNvarchar () {
        testConvertSchema (
            new Column(0, "TEST", "NVARCHAR", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertNvarcharNullable () {
        testConvertSchema (
            new Column(0, "TEST", "NVARCHAR", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateNvarchar () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "NVARCHAR", -1, -1, false),
            SchemaBuilder.string().defaultValue("").build()
        );      
    }
  
    @Test
    public void testConvertNchar () {
        testConvertSchema (
            new Column(0, "TEST", "NCHAR", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertNcharNullable () {
        testConvertSchema (
            new Column(0, "TEST", "NCHAR", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateNchar () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "NCHAR", -1, -1, false),
            SchemaBuilder.string().defaultValue("").build()
        );      
    }
  
    @Test
    public void testConvertLong () {
        testConvertSchema (
            new Column(0, "TEST", "LONG", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertLongNullable () {
        testConvertSchema (
            new Column(0, "TEST", "LONG", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateLong () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "LONG", -1, -1, false),
            SchemaBuilder.string().defaultValue("").build()
        );      
    }
  
    @Test
    public void testConvertIntervalDayToSecond () {
        testConvertSchema (
            new Column(0, "TEST", "INTERVAL DAY TO SECOND", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertIntervalDayToSecondNullable () {
        testConvertSchema (
            new Column(0, "TEST", "INTERVAL DAY TO SECOND", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateIntervalDayToSecond () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "INTERVAL DAY TO SECOND", -1, -1, false),
            SchemaBuilder.string().defaultValue("").build()
        );      
    }
  
    @Test
    public void testConvertIntervalDayToMonth () {
        testConvertSchema (
            new Column(0, "TEST", "INTERVAL YEAR TO MONTH", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertIntervalDayToMonthNullable () {
        testConvertSchema (
            new Column(0, "TEST", "INTERVAL YEAR TO MONTH", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateIntervalDayToMonth () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "INTERVAL YEAR TO MONTH", -1, -1, false),
            SchemaBuilder.string().defaultValue("").build()
        );      
    }
    
    @Test
    public void testConvertClob () {
        testConvertSchema (
            new Column(0, "TEST", "CLOB", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertClobNullable () {
        testConvertSchema (
            new Column(0, "TEST", "CLOB", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateClob () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for BYTES */
        testUpdateSchema (
            new Column(0, "TEST", "CLOB", -1, -1, false),
            SchemaBuilder.string()
                         .defaultValue("")
                         .build()
        );      
    }
  
    @Test
    public void testConvertNclob () {
        testConvertSchema (
            new Column(0, "TEST", "NCLOB", -1, -1, false),
            Schema.STRING_SCHEMA
        );
    }
    
    @Test
    public void testConvertNclobNullable () {
        testConvertSchema (
            new Column(0, "TEST", "NCLOB", -1, -1, true),
            Schema.OPTIONAL_STRING_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateNclob () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for BYTES */
        testUpdateSchema (
            new Column(0, "TEST", "NCLOB", -1, -1, false),
            SchemaBuilder.string()
                         .defaultValue("")
                         .build()
        );      
    }
    
    @Test
    public void testConvertDate () {
        testConvertSchema (
            new Column(0, "TEST", "DATE", -1, -1, false),
            Timestamp.builder().build()
        );
    }
    
    @Test
    public void testConvertDateNullable () {
        testConvertSchema (
            new Column(0, "TEST", "DATE", -1, -1, true),
            Timestamp.builder().optional().build()
        );
    }
    
    @Test 
    public void testUpdateDate () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for TIMESTAMP */
        testUpdateSchema (
            new Column(0, "TEST", "DATE", -1, -1, false),
            Timestamp.builder().defaultValue(new Date(0)).build()
        );      
    }
  
    @Test
    public void testConvertTimestamp () {
        testConvertSchema (
            new Column(0, "TEST", "TIMESTAMP", -1, -1, false),
            Timestamp.builder().build()
        );
    }
    
    @Test
    public void testConvertTimestampNullable () {
        testConvertSchema (
            new Column(0, "TEST", "TIMESTAMP", -1, -1, true),
            Timestamp.builder().optional().build()
        );
    }
    
    @Test 
    public void testUpdateTimestamp () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for TIMESTAMP */
        testUpdateSchema (
            new Column(0, "TEST", "TIMESTAMP", -1, -1, false),
            Timestamp.builder().defaultValue(new Date(0)).build()
        );      
    }
  
    @Test
    public void testConvertTimestampWithTZ () {
        testConvertSchema (
            new Column(0, "TEST", "TIMESTAMP WITH TIME ZONE", -1, -1, false),
            Timestamp.builder().build()
        );
    }
    
    @Test
    public void testConvertTimestampWithTZNullable () {
        testConvertSchema (
            new Column(0, "TEST", "TIMESTAMP WITH TIME ZONE", -1, -1, true),
            Timestamp.builder().optional().build()
        );
    }
    
    @Test 
    public void testUpdateTimestampWithTZ () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for TIMESTAMP */
        testUpdateSchema (
            new Column(0, "TEST", "TIMESTAMP WITH TIME ZONE", -1, -1, false),
            Timestamp.builder().defaultValue(new Date(0)).build()
        );      
    }
  
    @Test
    public void testConvertTimestampWithLocalTZ () {
        testConvertSchema (
            new Column(0, "TEST", "TIMESTAMP WITH LOCAL TIME ZONE", -1, -1, false),
            Timestamp.builder().build()
        );
    }
    
    @Test
    public void testConvertTimestampWithLocalTZNullable () {
        testConvertSchema (
            new Column(0, "TEST", "TIMESTAMP WITH LOCAL TIME ZONE", -1, -1, true),
            Timestamp.builder().optional().build()
        );
    }
    
    @Test 
    public void testUpdateTimestampWithLocalTZ () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT NULL for TIMESTAMP */
        testUpdateSchema (
            new Column(0, "TEST", "TIMESTAMP WITH LOCAL TIME ZONE", -1, -1, false),
            Timestamp.builder().defaultValue(new Date(0)).build()
        );      
    }
    
    @Test
    public void testConvertBlob () {
        testConvertSchema (
            new Column(0, "TEST", "BLOB", -1, -1, false),
            Schema.BYTES_SCHEMA
        );
    }
    
    @Test
    public void testConvertBlobNullable () {
        testConvertSchema (
            new Column(0, "TEST", "BLOB", -1, -1, true),
            Schema.OPTIONAL_BYTES_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateBlob () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT NULL for BYTES */
        testUpdateSchema (
            new Column(0, "TEST", "BLOB", -1, -1, false),
            SchemaBuilder.bytes()
                         .defaultValue(new byte[] {})
                         .build()
        );      
    }
  
    @Test
    public void testConvertRaw () {
        testConvertSchema (
            new Column(0, "TEST", "RAW", -1, -1, false),
            Schema.BYTES_SCHEMA
        );
    }
    
    @Test
    public void testConvertRawNullable () {
        testConvertSchema (
            new Column(0, "TEST", "RAW", -1, -1, true),
            Schema.OPTIONAL_BYTES_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateRaw () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT NULL for BYTES */
        testUpdateSchema (
            new Column(0, "TEST", "RAW", -1, -1, false),
            SchemaBuilder.bytes()
                         .defaultValue(new byte[] {})
                         .build()
        );      
    }
  
    @Test
    public void testConvertLongRaw () {
        testConvertSchema (
            new Column(0, "TEST", "LONG RAW", -1, -1, false),
            Schema.BYTES_SCHEMA
        );
    }
    
    @Test
    public void testConvertLongRawNullable () {
        testConvertSchema (
            new Column(0, "TEST", "LONG RAW", -1, -1, true),
            Schema.OPTIONAL_BYTES_SCHEMA
        );
    }
    
    @Test 
    public void testUpdateLongRaw () {
        /* force in NOT-NULL, expect DEFAULT VALUE and NOT-NULL for BYTES */
        testUpdateSchema (
            new Column(0, "TEST", "LONG RAW", -1, -1, false),
            SchemaBuilder.bytes()
                         .defaultValue(new byte[] {})
                         .build()
        );      
    }
    
    @SuppressWarnings("serial")
    public void testConvertSchema (
        final Column inColumnDef,
        final Schema outFieldSchema
    ) {
        try {
            MetaDataRecord mdr =
                MetaDataRecord.fromJSONString(METADATA_RECORD_NO_COLUMNS_JSON);
            
            /* add incoming column definition */
            mdr.getMetaData().getTableColumns().add (inColumnDef);
            mdr.getMetaData().getColumns().put (0, inColumnDef);
            
            final String mockName = "MOCK-SOE.UNITTEST"; 
            
            logger.info ("In: " + mdr.toJSONString());
            
            TopicSchema topicSchema =
                new ReplicateSchemaConverter()
                    .topicName(mockName)
                    .metadata(mdr.getMetaData())
                    .mode(defaultMode)
                    .convert();
            Schema schema = topicSchema.valueSchema();
            
            logger.info (
                "Out: schema name=" + schema.name() + " " +
                "type=" + schema.type()
            );
            
            validateSchema (
                mdr,
                schema,
                new LinkedHashMap<String, Schema>() {{
                    put ("XID", Schema.STRING_SCHEMA);
                    put ("TYPE", Schema.STRING_SCHEMA);
                    put ("CHANGE_ID", Schema.INT64_SCHEMA);
                    put (inColumnDef.getName(), outFieldSchema);
                }}
            );
            
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
    @SuppressWarnings("serial")
    public void testUpdateSchema (
        final Column inColumnDef,
        final Schema outFieldSchema
    ) {
        try {
            MetaDataRecord mdr =
                MetaDataRecord.fromJSONString(METADATA_RECORD_NO_COLUMNS_JSON);
            
            final String mockName = "MOCK-SOE.UNITTEST"; 
            
            logger.info ("In: " + mdr.toJSONString());
            
            TopicSchema topicSchema =
                new ReplicateSchemaConverter()
                    .topicName(mockName)
                    .metadata(mdr.getMetaData())
                    .mode(defaultMode)
                    .convert();
            
            Schema schema = topicSchema.valueSchema();
            
            /* add incoming column definition, pretend it's an update to
             * trigger default value
             */
            for (Column col : mdr.getMetaData().getTableColumns()) {
                /* set these columns as unchanged */
                col.setState(ColumnState.UNCHANGED);
            }
            mdr.getMetaData().getTableColumns().add (inColumnDef);
            mdr.getMetaData().getColumns().put (0, inColumnDef);
            
            inColumnDef.setState(ColumnState.ADDED);
            
            /* fake an update */
            topicSchema = new ReplicateSchemaConverter()
               .topicName(mockName)
               .schema(topicSchema)
               .metadata(mdr.getMetaData())
               .mode(defaultMode)
               .update();
            
            logger.info (
                "Out: schema name=" + schema.name() + " " +
                "type=" + schema.type()
            );
            
            /* overwrite with new updated value schema */
            schema = topicSchema.valueSchema();
            
            validateSchema (
                mdr,
                schema,
                new LinkedHashMap<String, Schema>() {{
                    put ("XID", Schema.STRING_SCHEMA);
                    put ("TYPE", Schema.STRING_SCHEMA);
                    put ("CHANGE_ID", Schema.INT64_SCHEMA);
                    put (inColumnDef.getName(), outFieldSchema);
                }}
            );
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
