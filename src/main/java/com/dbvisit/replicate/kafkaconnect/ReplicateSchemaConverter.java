package com.dbvisit.replicate.kafkaconnect;

import java.math.BigDecimal;
import java.util.Date;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.format.decoder.ColumnDataDecoder;
import com.dbvisit.replicate.plog.metadata.Column;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;

/** 
 * Allows replicate meta data to be converted to kafka schema or 
 * use replicate meta data to update kafka schema
 */
interface ReplicateMetaData {
    public ReplicateMetaData metadata (DDLMetaData metadata);
    public ReplicateMetaData schema (Schema schema);
    public Schema convert () throws Exception;
    public Schema update () throws Exception;
}

/**
 * Allows setting kafka topic name prior to processing replicate meta
 * data to a kafka schema, either new or updated
 */
interface KafkaTopicName {
    /* chain it to above interface */
    public ReplicateMetaData topicName (String topicName);
}

/** 
 * Simple converter class to take care of converting internal PLOG
 * schema meta data from DDL to kafka schemas, eg.
 * 
 * create schema
 * <pre>
 * new ReplicateSchemaConverter()
 *     .topicName("topic")
 *     .metadata(ddl)
 *     .convert();
 * </pre>
 * or update schema
 * <pre>
 * new ReplicateSchemaConverter()
 *     .topicName("topic")
 *     .schema(previous)
 *     .metadata(ddl).update();
 * </pre>
 */
public class ReplicateSchemaConverter
    implements KafkaTopicName, ReplicateMetaData
{
    private static final Logger logger = LoggerFactory.getLogger(
        ReplicateSchemaConverter.class
    );
    /** Replicate meta data to convert to a kafka schema */
    private DDLMetaData metadata;
    /** Kafka schema to update with replicate meta data */
    private Schema schema;
    /** The name of the topic */
    private String topicName;
    
    /**
     * Set the PLOG DDL meta data to convert to a kafka schema
     * 
     * @param metadata PLOG DDL meta data for replicated schema
     * 
     * @return ReplicateMetaData for chaining calls in builder-type pattern 
     */
    @Override
    public ReplicateMetaData metadata (DDLMetaData metadata) {
        this.metadata = metadata;
        
        return this;
    }

    /**
     * Set the kafka topic name for schema to create/update
     * 
     * @param topicName kafka topic name to publish data to
     * 
     * @return ReplicateMetaData for chaining calls in builder-type pattern 
     */
    @Override
    public ReplicateMetaData topicName (String topicName) {
        this.topicName = topicName;
        
        return this;
    }

    /**
     * Set the previous kafka schema to update with PLOG DDL schema meta data
     * 
     * @param schema previous kafka schema for this topic
     * 
     * @return ReplicateMetaData for chaining calls in builder-type pattern 
     */
    @Override
    public ReplicateMetaData schema (Schema schema) {
        this.schema = schema;
        
        return this;
    }
    /**
     * Convert PLOG schema metadata to kafka schema record
     * 
     * @return A kafka schema
     * @throws Exception for any conversion error
     */
    @Override
    public Schema convert () throws Exception {
        if (topicName == null) {
            throw new Exception (
                "Cannot convert meta data with no topic name"
            );
        }
        if (metadata == null) {
            throw new Exception (
                "Cannot convert non-existing replicate meta data record"
            );
        }
        
        SchemaBuilder builder = SchemaBuilder.struct().name(topicName);
        
        /* first field is transaction id */
        builder.field (
            ReplicateSourceTask.METADATA_TRANSACTION_ID_FIELD,
            Schema.STRING_SCHEMA
        );
        
        /* second field is CDR type */
        builder.field (
            ReplicateSourceTask.METADATA_CHANGE_TYPE_FIELD,
            Schema.STRING_SCHEMA
        );
        
        /* third fields is CDR ID */
        builder.field (
            ReplicateSourceTask.METADATA_CHANGE_ID_FIELD,
            Schema.INT64_SCHEMA
        );
        
        /* next all of the CDR values */
        for (Column column: metadata.getColumns().values()) {
            buildFieldFromColumn (builder, column);
        }

        return builder.build();
    }
    
    /**
     * Build a kafka schema field from an internal column meta data
     * record
     * 
     * @param builder the parent schema builder to use
     * @param column  the incoming PLOG meta data column
     */
    private void buildFieldFromColumn (
            SchemaBuilder builder, 
            Column column
    ) {
        buildFieldFromColumn (builder, column, false);
    }
    
    /**
     * Build a kafka schema field with a default value from column meta data
     * 
     * @param builder        the parent schema builder to use for building
     *                       field
     * @param column         column meta data read from PLOG
     * @param useNullDefault true or false, whether or not to use a default
     *                       in kafka schema field definition
     */
    private void buildFieldFromColumn (
        SchemaBuilder builder, 
        Column column,
        boolean useNullDefault
    ) {
        Schema fieldSchema = null; 
        String columnType = column.getType();
        
        boolean isNullable = true;
        if (column.getNullable() != null) {
            isNullable = column.getNullable();
        }
        
        switch (columnType) {
            case "NUMBER":
            {
                int scale = column.getScale();
                int precision = column.getPrecision();
                
                if (useNullDefault) {
                    fieldSchema = 
                        buildDecimalSchemaWithNullDefault(scale, precision);
                }
                else {
                    fieldSchema =
                        buildDecimalSchema(scale, precision, isNullable);
                }
                break;
            }
            case "VARCHAR2":
            case "VARCHAR":
            case "CHAR":
            case "NVARCHAR2":
            case "NVARCHAR":
            case "NCHAR":
            case "LONG":
            case "CLOB":
            case "CLOB_UTF16":
            case "NCLOB":
            case "INTERVAL DAY TO SECOND":
            case "INTERVAL YEAR TO MONTH":
            {
                if (useNullDefault) {
                    fieldSchema = buildStringSchemaWithNullDefault();
                }
                else {
                    fieldSchema = buildStringSchema (isNullable);
                }
                break;
            }
            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
            {
                if (useNullDefault) {
                    fieldSchema = buildTimestampSchemaWithNullDefault();
                }
                else {
                    fieldSchema = buildTimestampSchema (isNullable);
                }
                break;
            }
            case "BLOB":
            case "RAW":
            case "LONG RAW":
            {
                if (useNullDefault) {
                    fieldSchema = buildBytesSchemaWithNullDefault();
                }
                else {
                    fieldSchema = buildBytesSchema(isNullable);
                }
                break;
            }
            default:
            {
                logger.error(columnType + " is currently not supported.");
                fieldSchema = Schema.BYTES_SCHEMA;
                
                break;
            }
        }
        
        builder.field (column.getSafeName(), fieldSchema);
    }
    
    /**
     * Build decimal schema from scale and precision
     * 
     * @param scale      The numeric scale
     * @param precision  The numeric precision
     * @param isNullable Whether or not the field is nullable
     * 
     * @return Kafka decimal schema
     */
    private Schema buildDecimalSchema (
        int scale,
        int precision,
        boolean isNullable
    ) {
        Schema fieldSchema = null;
        
        if (
            scale > 0 ||
            precision <= 0 ||
            precision - scale > ColumnDataDecoder.NUMBER_LONG_MAX_PRECISION
        ) {
            /* decimal type */
            SchemaBuilder fieldBuilder = Decimal.builder(scale);
            if (isNullable) {
                fieldBuilder.optional();
            }
            fieldSchema = fieldBuilder.build();
        }
        else if (
            scale <= 0 && 
            precision - Math.abs (scale) < 
                ColumnDataDecoder.NUMBER_INTEGER_MAX_PRECISION
        ) {
            /* integer data type */
            if (isNullable) {
                fieldSchema = Schema.OPTIONAL_INT32_SCHEMA;
            }
            else {
                fieldSchema = Schema.INT32_SCHEMA;
            }
        }
        else {
            /* long data type */
            if (isNullable) {
                fieldSchema = Schema.OPTIONAL_INT64_SCHEMA;
            }
            else {
                fieldSchema = Schema.INT64_SCHEMA;
            }
        }
        
        return fieldSchema;
    }
    
    /**
     * Build decimal schema from scale and precision with null default value
     * 
     * @param scale      The numeric scale
     * @param precision  The numeric precision
     * 
     * @return Kafka decimal schema with null default for backwards
     *         compatibility
     */
    private Schema buildDecimalSchemaWithNullDefault (
        int scale,
        int precision
    ) {
        Schema fieldSchema = null;

        if (
            scale > 0 ||
            precision - scale > ColumnDataDecoder.NUMBER_LONG_MAX_PRECISION
        ) {
            /* decimal type */
            fieldSchema = 
                Decimal.builder(scale)
                       .optional()
                       .defaultValue(BigDecimal.ZERO)
                       .build();
        }
        else if (
            scale <= 0 && 
            precision - Math.abs (scale) < 
                ColumnDataDecoder.NUMBER_INTEGER_MAX_PRECISION
        ) {
            /* integer data type */
            fieldSchema = 
                SchemaBuilder.int32()
                             .optional()
                             .defaultValue(-1)
                             .build();
        }
        else {
            /* long data type */
            fieldSchema = 
                SchemaBuilder.int64()
                             .optional()
                             .defaultValue(-1L)
                             .build();
        }

        return fieldSchema;
    }
    
    /**
     * Build Kafka string schema definition
     * 
     * @param isNullable Whether or not the field is nullable
     * 
     * @return Kafka string schema
     */
    private Schema buildStringSchema (boolean isNullable) {
        Schema fieldSchema = null;
        
        if (isNullable) {
            fieldSchema = Schema.OPTIONAL_STRING_SCHEMA;
        } else {
            fieldSchema = Schema.STRING_SCHEMA;
        }
        
        return fieldSchema;
    }
    
    /**
     * Build Kafka string schema definition supporting default values
     * 
     * @return Kafka string schema
     */
    private Schema buildStringSchemaWithNullDefault () {
        return 
            SchemaBuilder.string()
                         .optional()
                         .defaultValue (new String(""))
                         .build();
    }
    
    /**
     * Build Kafka timestamp schema definition
     * 
     * @param isNullable Whether or not the field is nullable
     * 
     * @return Kafka timestamp schema
     */
    private Schema buildTimestampSchema (boolean isNullable) {
        SchemaBuilder fieldBuilder = Timestamp.builder();
        if (isNullable) {
            fieldBuilder.optional();
        }
        
        return fieldBuilder.build();
    }
    
    /**
     * Build Kafka timestamp schema definition supporting default values
     * 
     * @return Kafka timestamp schema with default value of start of epoch
     */
    private Schema buildTimestampSchemaWithNullDefault () {
        return
            Timestamp.builder()
                     .optional()
                     .defaultValue(new Date(0))
                     .build();
    }
    
    /**
     * Build Kafka raw bytes schema definition
     * 
     * @param isNullable Whether or not the field is nullable
     * 
     * @return Kafka bytes schema
     */
    private Schema buildBytesSchema (boolean isNullable) {
        Schema fieldSchema = null;
        
        if (isNullable) {
            fieldSchema = Schema.OPTIONAL_BYTES_SCHEMA;
        } else {
            fieldSchema = Schema.BYTES_SCHEMA;
        }
        
        return fieldSchema;
    }
    
    /**
     * Build Kafka raw bytes schema definition supporting default values
     * 
     * @return Kafka bytes schema with default empty value
     */
    private Schema buildBytesSchemaWithNullDefault () {
        return 
            SchemaBuilder.bytes()
                         .optional()
                         .defaultValue(new byte[] {})
                         .build();
    }
    
    /**
     * Update the kafka schema definition from updated PLOG meta data, only
     * adding or removing columns are currently supported.
     * 
     * @return Return new version of schema
     * @throws Exception for any conversion error
     */
    @Override
    public Schema update () throws Exception {
        if (topicName == null) {
            throw new Exception (
                "Cannot convert meta data with no topic name"
            );
        }
        if (metadata == null) {
            throw new Exception (
                "Cannot convert non-existing replicate meta data record"
            );
        }
        if (schema == null) {
            throw new Exception (
                "Cannot update non-existing kafka schema"
            );
        }
        
        SchemaBuilder builder = SchemaBuilder.struct().name(topicName);
        
        /* first field is transaction id */
        builder.field (
            ReplicateSourceTask.METADATA_TRANSACTION_ID_FIELD,
            Schema.STRING_SCHEMA
        );
        
        /* second field is CDR type */
        builder.field (
            ReplicateSourceTask.METADATA_CHANGE_TYPE_FIELD,
            Schema.STRING_SCHEMA
        );
        
        /* third fields is CDR ID */
        builder.field (
            ReplicateSourceTask.METADATA_CHANGE_ID_FIELD,
            Schema.INT64_SCHEMA
        );
        
        /* next all of the CDR values, check for updates */
        for (Column column: metadata.getColumns().values()) {
            Field prevField = schema.field(column.getSafeName());
            
            if (prevField == null) {
                /* new field, add default value of EMPTY, assume it's 
                 * non-mandatory as set when DDL meta data was updated
                 */
                buildFieldFromColumn (builder, column, true);
            }
            else {
                buildFieldFromColumn (builder, column);
            }
        }
        
        return builder.build();
    }
}
