package com.dbvisit.replicate.kafkaconnect;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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

/**
 *  Test schema conversion only
 */
public class ReplicateSchemaConversionTest extends ReplicateTestConfig {
    private static final Logger logger = LoggerFactory.getLogger (
        ReplicateSchemaConversionTest.class
    );
    
    /** always 3 extra fields in payload to link message to TX */
    final private int NUM_META_FIELDS = 3;
    final private String mockName = "MOCK-SOE.UNITTEST";

    @SuppressWarnings("serial")
    @Test
    public void testCreateAndUpdateSchema() {
        try {
            final String mockName = "MOCK-SOE.UNITTEST"; 
            
            /* create and verify first version of schema */
            MetaDataRecord mdr1 =
                MetaDataRecord.fromJSONString(METADATA_RECORD_1_JSON);
            
            Schema schema1 =
                new ReplicateSchemaConverter()
                    .topicName(mockName)
                    .metadata(mdr1.getMetaData())
                    .convert();
            
            logger.info ("In: " + mdr1.toJSONString());
            
            logger.info (
                "Out: schema name=" + schema1.name() + " " +
                "type=" + schema1.type()
            );
            
            validateSchema (
                mdr1, 
                schema1, 
                new LinkedHashMap<String, Schema>() {{
                    put ("XID", Schema.STRING_SCHEMA);
                    put ("TYPE", Schema.STRING_SCHEMA);
                    put ("CHANGE_ID", Schema.INT64_SCHEMA);
                    put ("ID", Schema.INT32_SCHEMA);
                    put ("TEST_NAME", Schema.OPTIONAL_STRING_SCHEMA);
                }}    
            );
            
            /* next create v2 of schema using v1 above and verify */
            MetaDataRecord mdr2 =
                MetaDataRecord.fromJSONString(METADATA_RECORD_2_JSON);
            
            Schema schema2 =
                new ReplicateSchemaConverter()
                    .topicName(mockName)
                    .schema(schema1)
                    .metadata(mdr2.getMetaData())
                    .update();
            
            logger.info ("In: " + mdr2.toJSONString());
            
            logger.info (
                "Out: schema name=" + schema2.name() + " " +
                "type=" + schema2.type()
            );
            
            validateSchema (
                mdr2,
                schema2,
                new LinkedHashMap<String, Schema>() {{
                    put ("XID", Schema.STRING_SCHEMA);
                    put ("TYPE", Schema.STRING_SCHEMA);
                    put ("CHANGE_ID", Schema.INT64_SCHEMA);
                    put ("ID", Schema.INT32_SCHEMA);
                    put ("TEST_NAME", Schema.OPTIONAL_STRING_SCHEMA);
                    /* decimal encoded as BYTES, in DDL it's mandatory field,
                     * but in schema it has to have default value and be
                     * nullable */
                    put (
                        "SCORE", 
                        Decimal.builder(4)
                               .optional()
                               .defaultValue(BigDecimal.ZERO)
                               .build()
                    );
                }}
            );
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
    @SuppressWarnings("serial")
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
        
        int expectedNumFields = expectedColumns.size() + NUM_META_FIELDS;
        
        assertTrue (
            "Expecting " + expectedNumFields + ", got: " +
            schema.fields().size(),
            schema.fields().size() == expectedNumFields    
        );
        
        /* extra in all of the payload and must always be present */
        List <String> expectedFields = new LinkedList<String>() {{
            add ("XID");
            add ("TYPE");
            add ("CHANGE_ID");
        }};
        
        for (Column expectedColumn : expectedColumns) {
            expectedFields.add (expectedColumn.getName());
        }
        
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
                            expectedColumns.get(i - NUM_META_FIELDS).getType()
                                .equals("NUMBER")
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
                            !expectedColumns.get(i - NUM_META_FIELDS).getType()
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
        /* force in NOT-NULL, expect DEFAULT VALUE -1 and NULLABLE for INT32 */
        testUpdateSchema (
            new Column(0, "TEST", "NUMBER", 6, 0, false),
            SchemaBuilder.int32().optional().defaultValue(-1).build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE -1 and NULLABLE for INT64 */
        testUpdateSchema (
            new Column(0, "TEST", "NUMBER", 18, 0, false),
            SchemaBuilder.int64().optional().defaultValue(-1L).build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE -1 and NULLABLE for INT64 */
        testUpdateSchema (
            new Column(0, "TEST", "NUMBER", 28, 10, false),
            Decimal.builder(10)
                   .optional()
                   .defaultValue(new BigDecimal(0))
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "VARCHAR2", -1, -1, false),
            SchemaBuilder.string().optional().defaultValue("").build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "VARCHAR", -1, -1, false),
            SchemaBuilder.string().optional().defaultValue("").build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "CHAR", -1, -1, false),
            SchemaBuilder.string().optional().defaultValue("").build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "NVARCHAR2", -1, -1, false),
            SchemaBuilder.string().optional().defaultValue("").build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "NVARCHAR", -1, -1, false),
            SchemaBuilder.string().optional().defaultValue("").build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "NCHAR", -1, -1, false),
            SchemaBuilder.string().optional().defaultValue("").build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "LONG", -1, -1, false),
            SchemaBuilder.string().optional().defaultValue("").build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "INTERVAL DAY TO SECOND", -1, -1, false),
            SchemaBuilder.string().optional().defaultValue("").build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for STRING */
        testUpdateSchema (
            new Column(0, "TEST", "INTERVAL YEAR TO MONTH", -1, -1, false),
            SchemaBuilder.string().optional().defaultValue("").build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for BYTES */
        testUpdateSchema (
            new Column(0, "TEST", "CLOB", -1, -1, false),
            SchemaBuilder.string()
                         .optional()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for BYTES */
        testUpdateSchema (
            new Column(0, "TEST", "NCLOB", -1, -1, false),
            SchemaBuilder.string()
                         .optional()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for TIMESTAMP */
        testUpdateSchema (
            new Column(0, "TEST", "DATE", -1, -1, false),
            Timestamp.builder().optional().defaultValue(new Date(0)).build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for TIMESTAMP */
        testUpdateSchema (
            new Column(0, "TEST", "TIMESTAMP", -1, -1, false),
            Timestamp.builder().optional().defaultValue(new Date(0)).build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for TIMESTAMP */
        testUpdateSchema (
            new Column(0, "TEST", "TIMESTAMP WITH TIME ZONE", -1, -1, false),
            Timestamp.builder().optional().defaultValue(new Date(0)).build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for TIMESTAMP */
        testUpdateSchema (
            new Column(0, "TEST", "TIMESTAMP WITH LOCAL TIME ZONE", -1, -1, false),
            Timestamp.builder().optional().defaultValue(new Date(0)).build()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for BYTES */
        testUpdateSchema (
            new Column(0, "TEST", "BLOB", -1, -1, false),
            SchemaBuilder.bytes()
                         .optional()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for BYTES */
        testUpdateSchema (
            new Column(0, "TEST", "RAW", -1, -1, false),
            SchemaBuilder.bytes()
                         .optional()
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
        /* force in NOT-NULL, expect DEFAULT VALUE and NULLABLE for BYTES */
        testUpdateSchema (
            new Column(0, "TEST", "LONG RAW", -1, -1, false),
            SchemaBuilder.bytes()
                         .optional()
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
            
            Schema schema =
                new ReplicateSchemaConverter()
                    .topicName(mockName)
                    .metadata(mdr.getMetaData())
                    .convert();
            
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
            
            Schema schema =
                new ReplicateSchemaConverter()
                    .topicName(mockName)
                    .metadata(mdr.getMetaData())
                    .convert();
            
            /* add incoming column definition, pretend it's an update to
             * trigger default value
             */
            mdr.getMetaData().getTableColumns().add (inColumnDef);
            mdr.getMetaData().getColumns().put (0, inColumnDef);
            
            /* fake an update */
            schema = new ReplicateSchemaConverter()
               .topicName(mockName)
               .schema(schema)
               .metadata(mdr.getMetaData())
               .update();
            
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

}
