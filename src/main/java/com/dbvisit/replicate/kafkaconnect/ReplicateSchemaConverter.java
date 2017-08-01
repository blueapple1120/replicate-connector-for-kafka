package com.dbvisit.replicate.kafkaconnect;

/**
 * Copyright 2016 Dbvisit Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import java.math.BigDecimal;
import java.util.Collection;
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
    public ReplicateMetaData schema (TopicSchema schema);
    public ReplicateMetaData mode(ConnectorMode mode);
    public TopicSchema convert () throws Exception;
    public TopicSchema update () throws Exception;
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
 *     .mode(connectorMode)
 *     .convert(isChangeSet);
 * </pre>
 * or update schema
 * <pre>
 * new ReplicateSchemaConverter()
 *     .topicName("topic")
 *     .schema(previous)
 *     .metadata(ddl)
 *     .mode(connectorMode)
 *     .update(isChangeSet);
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
    /** Topic schema to update with replicate meta data */
    private TopicSchema schema;
    /** The name of the topic */
    private String topicName;
    /** Connector mode of operation */
    private ConnectorMode mode;
    
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
    public ReplicateMetaData schema (TopicSchema schema) {
        this.schema = schema;
        
        return this;
    }
    
    /**
     * Set the mode of operation for connector, it determines what type
     * of message is build
     * 
     * @param mode connector operation mode
     */
    @Override
    public ReplicateMetaData mode(ConnectorMode mode) {
        this.mode = mode;
        return this;
    }
    
    /**
     * Convert PLOG schema metadata to kafka schema record
     * 
     * @return A topic schema consisting of optional key and mandatory value
     *         schema
     * @throws Exception for any conversion error
     */
    @Override
    public TopicSchema convert () throws Exception {
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
        if (mode == null) {
            throw new Exception (
                "Cannot convert meta data without connector mode configuration"
            );
        }
        
        /* key and value schema builder */
        SchemaBuilder kbuilder = mode.publishKeys()
                                 ? SchemaBuilder.struct().name(topicName)
                                 : null;
        SchemaBuilder vbuilder = SchemaBuilder.struct().name(topicName);
        
        /* add fields to link to transaction topic if configured to publish
         * these */
        if (mode.publishTxInfo()) {
            /* first field is transaction id */
            vbuilder.field (
                TopicSchema.METADATA_TRANSACTION_ID_FIELD,
                Schema.STRING_SCHEMA
            );
            
            /* second field is CDR type */
            vbuilder.field (
                TopicSchema.METADATA_CHANGE_TYPE_FIELD,
                Schema.STRING_SCHEMA
            );
            
            /* third fields is CDR ID */
            vbuilder.field (
                TopicSchema.METADATA_CHANGE_ID_FIELD,
                Schema.INT64_SCHEMA
            );
        }
        
        if (logger.isTraceEnabled()) {
            logger.trace(
                "Preparing schema for topic: " + topicName + " " +
                "CDC message format: " + mode.getCDCFormat()
            );
        }
        
        /* build different type of CDC messages */
        switch (mode.getCDCFormat()) {
            case CHANGEROW:
                buildChangeRowFields(kbuilder, vbuilder);
                break;
            case CHANGESET:
                buildChangeSetFields(kbuilder, vbuilder);
                break;
            default:
                break;
        }
        
        Schema kschema = mode.publishKeys()
                         ? kbuilder.build()
                         : null;
        Schema vschema = vbuilder.build();
        
        return new TopicSchema (kschema, vschema);
    }
    
    /**
     * Build the fields for a change row kafka key and message
     * 
     * @param kbuilder schema builder to build key fields
     * @param vbuilder schema builder to build message fields
     */
    private void buildChangeRowFields (
        SchemaBuilder kbuilder,        
        SchemaBuilder vbuilder
    ) {
        /* build all of the change row column values */
        for (Column column: metadata.getColumns().values()) {
            /* when publishing keys add only key columns or all columns when
             * table has no key constraints present and the column meets the
             * suplog requirements
             */
            if (mode.publishKeys() && 
                (column.isKey() 
                 || (!metadata.hasKey() && column.canUseAsSuplogKey())
                ))
            {
                if (logger.isDebugEnabled()) {
                    if (metadata.hasKey()) {
                        logger.debug (
                            "Adding key column: " + column.getSafeName() +
                            " of schema: " + metadata.getSchemataName()
                        );
                    }
                    else {
                        logger.debug (
                            "Adding non-key column: " + column.getSafeName() +
                            " as suplog key for schema: " + 
                            metadata.getSchemataName()       
                        );
                    }
                }
                
                buildFieldFromColumn (kbuilder, column);
            }
            
            buildFieldFromColumn (vbuilder, column);
        }
    }
    
    /**
     * Build the fields for a change set kafka message
     * 
     * @param kbuilder schema builder to build key fields
     * @param vbuilder schema builder to build message fields
     */
    private void buildChangeSetFields (
        SchemaBuilder kbuilder,
        SchemaBuilder vbuilder
    ) {
        Schema keySchema = Schema.STRING_SCHEMA;
        Schema valueSchema;
        
        /* sub struct builder force all fields as optional */
        SchemaBuilder sbuilder = 
            SchemaBuilder.struct().name ("CHANGE_SET");
        
        for (Column column: metadata.getColumns().values()) {
            /* when publishing keys add only key columns or all columns when
             * table has no key constraints present and the column meets the
             * suplog requirements
             */
            if (mode.publishKeys() &&
                (column.isKey() 
                 || (!metadata.hasKey() && column.canUseAsSuplogKey())
                ))
            {
                logger.debug (
                    "Adding column: " + column.getSafeName() + " as key"
                );
                buildFieldFromColumn (kbuilder, column);
            }

            /* optional field for all KEY/NEW/OLD/LOB values in map */
            buildFieldFromColumn (sbuilder, column, true);
        }
        
        valueSchema = sbuilder.build();
        
        vbuilder.field (
            "CHANGE_DATA",
            SchemaBuilder.map (keySchema, valueSchema)
        );
    }
    
    /**
     * Build a kafka schema field from an internal column meta data record
     * 
     * @param builder the parent schema builder to use
     * @param column  the incoming PLOG meta data column
     */
    private void buildFieldFromColumn (
        SchemaBuilder builder, 
        Column column
    ) {
        buildFieldFromColumn (builder, column, false, false);
    }
    
    
    /**
     * Build a kafka schema field from an internal column meta data record
     * 
     * @param builder       the parent schema builder to use
     * @param column        the incoming PLOG meta data column
     * @param forceOptional force schema field to be optional, overwrite the
     *                      nullable property of column
     */
    private void buildFieldFromColumn (
        SchemaBuilder builder, 
        Column        column,
        boolean       forceOptional
    ) {
        buildFieldFromColumn (builder, column, forceOptional, false);
    }
    
    /**
     * Build a kafka schema field from an internal column meta data record
     * 
     * @param builder        the parent schema builder to use for building field
     * @param column         column meta data read from PLOG
     * @param forceOptional  force schema field to be optional, overwrite the
     *                       nullable property of column
     * @param forceBackwards whether or not this update for field will be new
     *                       version that needs to be backwards compatible
     */
    private void buildFieldFromColumn (
        SchemaBuilder builder, 
        Column        column,
        boolean       forceOptional,
        boolean       forceBackwards
    ) {
        Schema fieldSchema = null; 
        String columnType = column.getType();
        
        boolean optional = true;
        if (!forceOptional && column.isNullable() != null) {
            optional = column.isNullable();
        }
        boolean backwards = forceBackwards;
        
        switch (columnType) {
            case "NUMBER":
            {
                int scale     = column.getScale();
                int precision = column.getPrecision();
                
                fieldSchema =
                    buildDecimalSchema(scale, precision, optional, backwards);
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
                fieldSchema = buildStringSchema (optional, backwards);
                break;
            }
            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
            {
                fieldSchema = buildTimestampSchema (optional, backwards);
                break;
            }
            case "BLOB":
            case "RAW":
            case "LONG RAW":
            {
                fieldSchema = buildBytesSchema(optional, backwards);
                break;
            }
            default:
            {
                logger.error(columnType + " is currently not supported.");
                fieldSchema = Schema.BYTES_SCHEMA;
                
                break;
            }
        }
        
        if (logger.isTraceEnabled()) {
            logger.trace(
                "Preparing schema field for " + 
                "column: " + column.getName() + " " + 
                "key: " + column.isKey() + " " +
                "type: " + columnType + " " + 
                "nullable: " + optional + " " +
                "precision: " + column.getPrecision() + " " +
                "scale: " + column.getScale() + " " +
                "backwards compatible: " + forceBackwards + ". " +
                "Output schema: " + fieldSchema.type().toString()
            );
        }
        builder.field (column.getSafeName(), fieldSchema);
    }
    
    /**
     * Build decimal schema from scale and precision
     * 
     * @param scale     The numeric scale
     * @param precision The numeric precision
     * @param optional  Whether or not the field is optional
     * @param backwards Whether or not this field needs to be backwards
     *                  compatible
     * 
     * @return Kafka decimal schema
     */
    private Schema buildDecimalSchema (
        int     scale,
        int     precision,
        boolean optional,
        boolean backwards
    ) {
        SchemaBuilder fieldBuilder = null;
        if (
            scale > 0 ||
            precision <= 0 ||
            precision - scale >= ColumnDataDecoder.NUMBER_LONG_MAX_PRECISION
        ) {
            /* decimal type */
            fieldBuilder = Decimal.builder(scale);
            if (optional) {
                fieldBuilder.optional();
            }
            if (backwards) {
                /* add zero default for missing values */
                fieldBuilder.defaultValue(BigDecimal.ZERO.setScale(scale));
            }
        }
        else if (
            scale <= 0 && 
            precision - Math.abs (scale) < 
                ColumnDataDecoder.NUMBER_INTEGER_MAX_PRECISION
        ) {
            /* integer data type */
            fieldBuilder = SchemaBuilder.int32();
            if (optional) {
                fieldBuilder.optional();
            }
            if (backwards) {
                /* add zero default for missing values */
                fieldBuilder.defaultValue(0);
            }
        }
        else {
            /* long data type */
            fieldBuilder = SchemaBuilder.int64();
            if (optional) {
                fieldBuilder.optional();
            }
            if (backwards) {
                /* add zero default for missing values */
                fieldBuilder.defaultValue(0L);
            }
        }
        return fieldBuilder.build();
    }
    
    /**
     * Build Kafka string schema definition
     * 
     * @param optional  Whether or not the field is optional
     * @param backwards Whether or not this field needs to be backwards
     *                  compatible
     * 
     * @return Kafka string schema
     */
    private Schema buildStringSchema (boolean optional, boolean backwards) {
        SchemaBuilder fieldBuilder = SchemaBuilder.string();
        if (optional) {
            fieldBuilder.optional();
        }
        if (backwards) {
            /* add empty string default for missing values */
            fieldBuilder.defaultValue(new String(""));
        }
        
        return fieldBuilder.build();
    }
        
    /**
     * Build Kafka timestamp schema definition
     * 
     * @param optional  Whether or not the field is optional
     * @param backwards Whether or not this field needs to be backwards
     *                  compatible
     * 
     * @return Kafka timestamp schema
     */
    private Schema buildTimestampSchema (boolean optional, boolean backwards) {
        SchemaBuilder fieldBuilder = Timestamp.builder();
        if (optional) {
            fieldBuilder.optional();
        }
        if (backwards) {
            /* add epoch start as default for missing values */
            fieldBuilder.defaultValue(new Date(0));
        }
        return fieldBuilder.build();
    }
    
    /**
     * Build Kafka raw bytes schema definition
     * 
     * @param optional  Whether or not the field is nullable
     * @param backwards Whether or not this field needs to be backwards
     *                  compatible
     * 
     * @return Kafka bytes schema
     */
    private Schema buildBytesSchema (boolean optional, boolean backwards) {
        SchemaBuilder fieldBuilder = SchemaBuilder.bytes();
        if (optional) {
            fieldBuilder.optional();
        }
        if (backwards) {
            /* add empty byte as default for missing values */
            fieldBuilder.defaultValue(new byte[] {});
        }
        
        return fieldBuilder.build();
    }
    
    /**
     * Update the kafka schema definition from updated PLOG meta data, only
     * adding or removing columns are currently supported.
     * 
     * @return Return new version of topic schema to replace original
     * @throws Exception for any conversion error
     */
    @Override
    public TopicSchema update () throws Exception {
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
        if (mode == null) {
            throw new Exception (
                "Cannot update meta data without connector mode configuration"
            );
        }
        
        /* key and value schema builders */
        SchemaBuilder kbuilder = mode.publishKeys() 
                                 ? SchemaBuilder.struct().name(topicName)
                                 : null;
        SchemaBuilder vbuilder = SchemaBuilder.struct().name(topicName);
        
        /* add fields to link to transaction topic if configured to publish
         * these */
        if (mode.publishTxInfo()) {
            /* first field is transaction id */
            vbuilder.field (
                TopicSchema.METADATA_TRANSACTION_ID_FIELD,
                Schema.STRING_SCHEMA
            );
            
            /* second field is CDR type */
            vbuilder.field (
                TopicSchema.METADATA_CHANGE_TYPE_FIELD,
                Schema.STRING_SCHEMA
            );
            
            /* third fields is CDR ID */
            vbuilder.field (
                TopicSchema.METADATA_CHANGE_ID_FIELD,
                Schema.INT64_SCHEMA
            );
        }
        
        Schema prevSchema = schema.valueSchema();
        
        if (logger.isTraceEnabled()) {
            logger.trace(
                "Updating schema for topic: " + topicName + " " +
                "CDC message format: " + mode.getCDCFormat()
            );
        }
        
        /* update different type of CDC messages */
        switch (mode.getCDCFormat()) {
            case CHANGEROW:
                updateChangeRowFields(prevSchema, kbuilder, vbuilder);
                break;
            case CHANGESET:
                updateChangeSetFields(prevSchema, kbuilder, vbuilder);
                break;
            default:
                break;
        }
        
        /* build key schema if configured to publish keys */
        Schema kschema = mode.publishKeys() 
                         ? kbuilder.build() 
                         : null;
        Schema vschema = vbuilder.build();
        
        return new TopicSchema (kschema, vschema);
    }
    
    /**
     * Update the fields for a change row kafka key and message
     * 
     * @param prevSchema previous schema to use in field comparison
     * @param keyBuilder   schema builder to build key fields
     * @param valueBuilder   schema builder to build message fields
     * 
     * @throws Exception 
     */
    private void updateChangeRowFields (
        Schema        prevSchema,
        SchemaBuilder keyBuilder,        
        SchemaBuilder valueBuilder
    ) throws Exception {
        /* next go through all of the column definitions, check for updates */
        updateSchemaFields(
            prevSchema, 
            keyBuilder, 
            valueBuilder, 
            metadata.getColumns().values()
        );
    }
    
    /**
     * Update the fields for a change set kafka message and key, if needed
     * 
     * @param prevSchema   previous schema to use in field comparison
     * @param keyBuilder   schema builder to build key fields
     * @param valueBuilder schema builder to build message fields
     */
    private void updateChangeSetFields (
        Schema        prevSchema,
        SchemaBuilder keyBuilder,
        SchemaBuilder valueBuilder
    ) throws Exception {
        Schema keySchema = Schema.STRING_SCHEMA;
        Schema valueSchema;
        
        /* sub struct builder force all fields as optional */
        SchemaBuilder subBuilder = 
            SchemaBuilder.struct().name ("CHANGE_SET");
        
        updateSchemaFields(
            prevSchema.field("CHANGE_DATA").schema().valueSchema(), 
            keyBuilder, 
            subBuilder, 
            metadata.getColumns().values()
        );
        
        valueSchema = subBuilder.build();
        
        valueBuilder.field (
            "CHANGE_DATA",
            SchemaBuilder.map (keySchema, valueSchema)
        );
    }
    
    /**
     * Common function to decide how to update a schema field
     * 
     * @param prevSchema   previous schema to use in field comparison
     * @param keyBuilder   schema builder to build key fields
     * @param valueBuilder schema builder to build message fields
     * @param columns      the columns to use for updating schema definition
     * 
     * @throws Exception for schema update errors
     */
    private void updateSchemaFields (
        Schema        prevSchema,
        SchemaBuilder keyBuilder,
        SchemaBuilder valueBuilder,
        Collection<Column>  columns
    ) throws Exception {
        
        /* next go through all of the column definitions, check for updates */
        for (Column column: columns) {
            switch (column.getState()) {
                case ADDED: 
                {
                    /* only publish new fields when in correct mode */
                    if (mode.noSchemaEvolution() == false) {
                        /* new field, add default value of zero aka empty */
                        buildFieldFromColumn (
                            valueBuilder, column, false, true
                        );
                    
                        /* new key field, update key schema and set it to be 
                         * backwards compatible, if configured to publish 
                         * record keys
                         */
                        if (mode.publishKeys() && column.isKey()) {
                            logger.debug (
                                "Updating key column: " + column.getSafeName()
                            );
                            buildFieldFromColumn (
                                keyBuilder, column, false, true
                            );
                        }
                    }
                    else {
                        logger.trace (
                            "Schema changes are disabled, ignoring new " +
                            "field: " + column.toString()
                        );
                    }
                    break;
                }
                case REMOVED:
                {
                    /* check if previous was optional or not */
                    Field prevField = prevSchema.field(column.getSafeName());
                    
                    if (prevField == null) {
                        throw new Exception (
                            "No existing schema field for removed " +
                            column.toString() + " for schema: " +
                            prevSchema.name()
                        );
                    }
                    
                    if (prevField.schema().isOptional()) {
                        /* keep value and record field as is */
                        valueBuilder.field (
                            prevField.name(), prevField.schema()
                        );
                        
                        if (mode.publishKeys() && column.isKey()) {
                            keyBuilder.field(
                                prevField.name(),
                                prevField.schema()
                            );
                        }
                    }
                    else {
                        /* force field as optional */
                        buildFieldFromColumn (valueBuilder, column, true);
                    }
                    break;
                }
                case MODIFIED:
                {
                    if (mode.noSchemaEvolution() == false) { 
                        logger.error ("Unsupported state:" + column.getState());
                    }
                    break;
                }
                case UNCHANGED:
                {
                    /* retain previous field definition as is */
                    Field prevField = prevSchema.field(column.getSafeName());
                    
                    if (prevField != null) 
                    {
                        /* copy field as is, if it existed */
                        valueBuilder.field (prevField.name(), prevField.schema());
                    
                        /* copy record key as is */
                        if (mode.publishKeys() && column.isKey()) {
                            keyBuilder.field(prevField.name(), prevField.schema());
                        }
                        
                    }
                    else if (mode.noSchemaEvolution() == false) {
                        /* abort if schema changes were allowed and a previous
                         * field not longer exists
                         */
                        throw new Exception (
                            "No existing schema field for unchanged " +
                            column.toString() + " for schema: " +
                            prevSchema.name()
                        );
                    }
                    else {
                        logger.trace (
                            "Schema changes are disabled, ignoring " +
                            "non-existing field: " + column.toString()
                        );
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }
}
