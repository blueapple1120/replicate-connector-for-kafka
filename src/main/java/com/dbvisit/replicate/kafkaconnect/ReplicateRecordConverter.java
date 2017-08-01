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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.serial.SerialBlob;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.ColumnValue;
import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.ChangeRowRecord;
import com.dbvisit.replicate.plog.domain.ChangeSetRecord;
import com.dbvisit.replicate.plog.domain.TransactionInfoRecord;

/**
 * Replicate record converter behavior
 */
interface ReplicateRecord {
    public ReplicateRecord mode(ConnectorMode mode);
    public ReplicateRecord record(DomainRecord record);
    public SourceRecord convert() throws Exception;
}

/**
 * Allows setting kafka schema prior to converting PLOG record to kafka
 * source record
 */
interface KafkaSchema {
    public ReplicateRecord schema(TopicSchema schema);
}

/**
 * Builder-style converter for PLOG replicate records to kafka source records,
 * eg. 
 * 
 * <pre>
 * new ReplicateRecordConverter()
 *     .schema(kakfaSchema)
 *     .mode(connectorMode)
 *     .record(plogRecord)
 *     .convert();
 * </pre>
 */
public class ReplicateRecordConverter implements ReplicateRecord, KafkaSchema 
{
    private static final Logger logger = LoggerFactory.getLogger(
        ReplicateRecordConverter.class
    );
    
    /** Domain record to convert */
    private DomainRecord record;
    /** Topic schema that defines the converted record key and value schema */
    private TopicSchema schema;
    /** Connector mode of operation */
    private ConnectorMode mode;

    /**
     * Set the kafka schema that hold the definition of the output kafka
     * source record
     * 
     * @param schema Kafka schema definition for kafka source record
     * 
     * @return ReplicateRecord for chaining calls in builder-type pattern 
     */
    @Override
    public ReplicateRecord schema (TopicSchema schema) {
        this.schema = schema;
        return this;
    }

    /**
     * Set the PLOG domain record to convert to kafka source record
     * 
     * @param record domain record to convert, either transaction info or
     *               change record
     *               
     * @return ReplicateRecord for chaining calls in builder-type pattern 
     */
    @Override
    public ReplicateRecord record (DomainRecord record) {
        this.record = record;
        return this;
    }
    
    @Override
    public ReplicateRecord mode(ConnectorMode mode) {
        this.mode = mode;
        return this;
    }

    /**
     * Convert replicate domain record to kafka source record
     * 
     * @return kafka source record
     * @throws Exception for any conversion error
     */
    @Override
    public SourceRecord convert() throws Exception {
        if (record == null) {
            throw new Exception ("Cannot convert non-existing domain record");
        }
        if (schema == null) {
            throw new Exception ("Cannot convert non-existing kafka schema");
        }
        if (record.getRecordOffset() == null) {
            throw new Exception ("Cannot convert domain record with no offset");
        }
        if (mode == null) {
            throw new Exception (
                "Cannot convert record without connector mode configuration"
            );
        }
        
        SourceRecord sourceRecord = null;
        
        if (logger.isTraceEnabled()) {
            logger.trace ("Converting: " + record.toJSONString());
        }
        
        switch (record.getDomainRecordType()) {
            case CHANGEROW_RECORD:
            {
                if (mode.getCDCFormat() == ConnectorCDCFormat.CHANGEROW) {
                    sourceRecord = convertChangeRowRecord (record);
                }
                else {
                    throw new Exception (
                        "Invalid configuration, not expecting "         + 
                        record.getDomainRecordType() + " records when " +
                        "configured for CDC format: " + mode.getCDCFormat()
                    );
                }
                break;
            }
            case CHANGESET_RECORD:
            {
                if (mode.getCDCFormat() == ConnectorCDCFormat.CHANGESET) {
                    sourceRecord = convertChangeSetRecord (record);
                }
                else {
                    throw new Exception (
                        "Invalid configuration, not expecting "         + 
                        record.getDomainRecordType() + " records when " +
                        "configured for CDC format: " + mode.getCDCFormat()
                    );
                }    
                break;
            }
            case TRANSACTION_INFO_RECORD:
            {
                if (mode.publishTxInfo()) {
                    sourceRecord = convertTransactionDataRecord (record);
                }
                else {
                    throw new Exception (
                        "Invalid configuration, not expecting "         + 
                        record.getDomainRecordType() + " records when " +
                        "configured to not publish transaction info "   +
                        "meta data messages"
                    );
                }
                break;
            }
            case METADATA_RECORD:
            case HEADER_RECORD:
            default:
                /* we do not convert these type of records to Kafka records */
                break;
        }

        return sourceRecord;
    }
    
    /**
     * Convert a single change row record to kafka source record
     * 
     * @param domainRecord A logical change record
     * 
     * @return A kakfa source record
     * @throws Exception Failed to convert a change row record
     */
    private SourceRecord convertChangeRowRecord (DomainRecord domainRecord) 
    throws Exception 
    {
        if (!domainRecord.isChangeRowRecord()) {
            throw new Exception (
                "Invalid change row domain record, reason type is " + 
                domainRecord.getDomainRecordType()
            );
        }
        ChangeRowRecord lcr = (ChangeRowRecord) domainRecord;

        if (!lcr.hasTableOwner()) {
            throw new Exception (
                "LCR: " + lcr.toJSONString() + " has no owner metadata"
            );
        }

        /* value schema and value */
        Schema vschema = schema.valueSchema();
        Struct vstruct = new Struct (vschema);
        
        Schema kschema = null;
        Struct kstruct = null;
        
        if (mode.publishKeys()) {
            /* set key schema and value for publishing keys */
            kschema = schema.keySchema();
            if (kschema == null) {
                throw new Exception (
                    "Connector is configured to publish keys, but none " +
                    "available for topic: " + vschema.name()
                );
            }
            kstruct = new Struct (kschema);
        }
        else {
            /* sanity check */
            if (schema.keySchema() != null) {
                logger.warn (
                    "Connector is configured to not publish keys, but key " +
                    "schema is available for topic: " + vschema.name()
                );
            }
        }
        
        String topic = vschema.name();
        String partKey = topic;

        /* add row meta data fields if the connector is configured to 
         * publish the transaction info meta data topic */
        if (mode.publishTxInfo()) {
            vstruct.put (
                TopicSchema.METADATA_TRANSACTION_ID_FIELD, 
                lcr.getTransactionId()
            );
            vstruct.put (
                TopicSchema.METADATA_CHANGE_TYPE_FIELD,
                lcr.getAction().toString()
            );
            vstruct.put (
                TopicSchema.METADATA_CHANGE_ID_FIELD,
                lcr.getId()
            );
        }

        /* convert all column values to Kafka struct fields */
        for (ColumnValue cdr : lcr.getColumnValues()) {
            if (cdr != null) {
                if (mode.noSchemaEvolution() == false || (
                        mode.noSchemaEvolution() && 
                        vschema.field(cdr.getName()) != null
                    )
                ) {
                    Object val = cdr.getValue();
    
                    /* convert LOBs */
                    if (cdr.getValue() instanceof SerialBlob) {
                        SerialBlob sb = (SerialBlob) val;
                        /* support empty BLOBs */
                        val = (
                            sb.length() == 0 
                            ? new byte [] {}
                            : sb.getBytes(1, (int)sb.length())
                        );
                    }
                    vstruct.put (cdr.getName(), val);
                    
                    /* add key values if configured to publish keys */
                    if (mode.publishKeys() && 
                        kschema.field(cdr.getName()) != null)
                    {
                        kstruct.put (cdr.getName(), val);
                    }
                }
            }
        }

        SourceRecord srecord = null;
        if (mode.publishKeys()) {
            srecord = new SourceRecord (
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_NAME_KEY, 
                    partKey
                ),
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_OFFSET_KEY,
                    lcr.getReplicateOffset().toJSONString()
                ),
                topic,
                null,
                kschema,
                kstruct,
                vschema, 
                vstruct,
                (Long)lcr.getTimestamp().getTime()
            );
        }
        else {
            srecord = new SourceRecord (
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_NAME_KEY, 
                    partKey
                ),
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_OFFSET_KEY,
                    lcr.getReplicateOffset().toJSONString()
                ),
                topic,
                null,
                null,
                null,
                vschema, 
                vstruct,
                (Long)lcr.getTimestamp().getTime()
            );
        }
        return srecord;
    }

    /**
     * Convert a single change set record to kafka source record
     * 
     * @param domainRecord A logical change record
     * 
     * @return A kakfa source record
     * @throws Exception Failed to convert a change set record
     */
    private SourceRecord convertChangeSetRecord (DomainRecord domainRecord) 
    throws Exception 
    {
        if (!domainRecord.isChangeSetRecord()) {
            throw new Exception (
                "Invalid change set domain record, reason type is " + 
                domainRecord.getDomainRecordType()
            );
        }
        final ChangeSetRecord csr = (ChangeSetRecord) domainRecord;

        if (!csr.hasTableOwner()) {
            throw new Exception (
                "LCR: " + csr.toJSONString() + " has no owner metadata"
            );
        }
        
        /* value schema and value */
        Schema vschema = schema.valueSchema();
        Struct vstruct = new Struct (vschema);
        
        Schema kschema = null;
        Struct kstruct = null;
        
        if (mode.publishKeys()) {
            /* set key schema and value for publishing keys */
            kschema = schema.keySchema();
            if (kschema == null) {
                throw new Exception (
                    "Connector is configured to publish keys, but none " +
                    "available for topic: " + vschema.name()
                );
            }
            kstruct = new Struct (kschema);
        }
        else {
            /* sanity check */
            if (schema.keySchema() != null) {
                logger.warn (
                    "Connector is configured to not publish keys, but key " +
                    "schema is available for topic: " + vschema.name()
                );
            }
        }
        
        String topic = vschema.name();
        String partKey = topic;

        /* add row meta data fields if the connector is configured to 
         * publish the transaction info meta data topic */
        if (mode.publishTxInfo()) {
            vstruct.put (
                TopicSchema.METADATA_TRANSACTION_ID_FIELD, 
                csr.getTransactionId()
            );
            vstruct.put (
                TopicSchema.METADATA_CHANGE_TYPE_FIELD,
                csr.getAction().toString()
            );
            vstruct.put (
                TopicSchema.METADATA_CHANGE_ID_FIELD,
                csr.getId()
            );
        }

        /* process all change values and add as embedded records */
        @SuppressWarnings("serial")
        final Map<String, List<ColumnValue>> processChangeValues = 
            new LinkedHashMap<String, List<ColumnValue>>() 
        {{
            if (csr.hasKeyValues()) {
                put ("KEY", csr.getKeyValues());
            }
            if (csr.hasOldValues()) {
                put ("OLD", csr.getOldValues());
            }
            if (csr.hasNewValues()) {
                put ("NEW", csr.getNewValues());
            }
            if (csr.hasLobValues()) {
                put ("LOB", csr.getLobValues());
            }
        }};
        
        Map<String, Struct> structMap = new LinkedHashMap <String, Struct>();
        
        Schema subschema = vschema.field("CHANGE_DATA").schema().valueSchema();
        for (String type : processChangeValues.keySet()) {
            Struct substruct = new Struct (subschema);
            
            for (ColumnValue cdr : processChangeValues.get (type)) {
                if (cdr != null) {
                    if (mode.noSchemaEvolution() == false || (
                            mode.noSchemaEvolution() && 
                            subschema.field(cdr.getName()) != null
                        )
                    ) {
                        Object val = cdr.getValue();
    
                        /* convert LOBs */
                        if (cdr.getValue() instanceof SerialBlob) {
                            SerialBlob sb = (SerialBlob) val;
                            /* support empty BLOBs */
                            val = (
                                sb.length() == 0 
                                ? new byte [] {}
                                : sb.getBytes(1, (int)sb.length())
                            );
                        }
                        substruct.put (cdr.getName(), val);
                        
                        /* add key values if configured to publish keys */
                        if (mode.publishKeys() && 
                            kschema.field(cdr.getName()) != null)
                        {
                            kstruct.put (cdr.getName(), val);
                        }
                    }
                }
            }
            structMap.put (type, substruct);
        }
        
        /* add change record as map */
        vstruct.put ("CHANGE_DATA", structMap);

        SourceRecord srecord = null;
        if (mode.publishKeys()) {
            srecord = new SourceRecord (
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_NAME_KEY, 
                    partKey
                ),
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_OFFSET_KEY,
                    csr.getReplicateOffset().toJSONString()
                ),
                topic,
                null,
                kschema,
                kstruct,
                vschema, 
                vstruct,
                (Long)csr.getTimestamp().getTime()
            );
        }
        else {
            srecord = new SourceRecord (
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_NAME_KEY, 
                    partKey
                ),
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_OFFSET_KEY,
                    csr.getReplicateOffset().toJSONString()
                ),
                topic, 
                null,
                null,
                null,
                vschema, 
                vstruct,
                (Long)csr.getTimestamp().getTime()
            );
        }
        return srecord;
    }
    
    
    /**
     * Convert PLOG transaction data record to Kafka SourceRecord
     * 
     * @param domainRecord PLOG domain record for transaction data
     * 
     * @return record converted to Kafka source record
     * @throws Exception upon failure
     */
    private SourceRecord convertTransactionDataRecord (
        DomainRecord domainRecord
    ) 
    throws Exception 
    {
        if (!domainRecord.isTransactionInfoRecord()) {
            throw new Exception (
                "Invalid transaction data record, reason type is " + 
                domainRecord.getDomainRecordType()
            );
        }
        
        TransactionInfoRecord txr = (TransactionInfoRecord) domainRecord;
        
        Struct kstruct = null;
        Schema kschema = null;
        Schema vschema = schema.valueSchema();
        String topic   = vschema.name();
        String partKey = topic;

        if (mode.publishKeys()) {
            /* set key schema and value for publishing keys */
            kschema = schema.keySchema();
            if (kschema == null) {
                throw new Exception (
                    "Connector is configured to publish keys, but none " +
                    "available for topic: " + vschema.name()
                );
            }
            kstruct = new Struct (kschema);
        }
        else {
            /* sanity check */
            if (schema.keySchema() != null) {
                logger.warn (
                    "Connector is configured to not publish keys, but key " +
                    "schema is available for topic: " + vschema.name()
                );
            }
        }
        
        if (mode.publishKeys()) {
            kstruct.put ("XID", txr.getId());
        }
        
        Struct vstruct = new Struct (vschema);
        
        vstruct.put ("XID", txr.getId());
        vstruct.put ("START_SCN", txr.getStartSCN());
        vstruct.put ("END_SCN", txr.getEndSCN());
        vstruct.put ("START_TIME", txr.getStartTime());
        vstruct.put ("END_TIME", txr.getEndTime());
        vstruct.put ("START_CHANGE_ID", txr.getStartRecordId());
        vstruct.put ("END_CHANGE_ID", txr.getEndRecordId());
        vstruct.put ("CHANGE_COUNT", txr.getRecordCount());
        
        /* array of structs to hold counts per replicated schema */
        List<Struct> sarray = new ArrayList<Struct>();
        Map<String, Integer> counts =  txr.getSchemaRecordCounts();
        for (String name : counts.keySet()) {
            Integer count = counts.get (name);
            Struct cstruct = new Struct (
                vschema.field("SCHEMA_CHANGE_COUNT_ARRAY")
                      .schema().valueSchema()
            );
            cstruct.put ("SCHEMA_NAME", name);
            cstruct.put ("CHANGE_COUNT", count);
            sarray.add (cstruct);
        }
       
        vstruct.put ("SCHEMA_CHANGE_COUNT_ARRAY", sarray);
        
        SourceRecord srecord = null;
        if (mode.publishKeys()) {
            srecord = new SourceRecord (
                    Collections.singletonMap(
                        ReplicateSourceTask.REPLICATE_NAME_KEY, 
                        partKey
                    ),
                    Collections.singletonMap(
                        ReplicateSourceTask.REPLICATE_OFFSET_KEY,
                        txr.getReplicateOffset().toJSONString()
                    ),
                    topic,
                    kschema,
                    kstruct,
                    vschema, 
                    vstruct
                );
        }
        else {
            srecord = new SourceRecord (
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_NAME_KEY, 
                    partKey
                ),
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_OFFSET_KEY,
                    txr.getReplicateOffset().toJSONString()
                ),
                topic, 
                vschema, 
                vstruct
            );
        }
        
        return srecord;
    }

}
