package com.dbvisit.replicate.kafkaconnect;

import java.util.ArrayList;
import java.util.Collections;
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
import com.dbvisit.replicate.plog.domain.LogicalChangeRecord;
import com.dbvisit.replicate.plog.domain.TransactionInfoRecord;

/**
 * Replicate record converter behavior
 */
interface ReplicateRecord {
    public ReplicateRecord record(DomainRecord record);
    public SourceRecord convert() throws Exception;
}

/**
 * Allows setting kafka schema prior to converting PLOG record to kafka
 * source record
 */
interface KafkaSchema {
    public ReplicateRecord schema(Schema schema);
}

/**
 * Builder-style converter for PLOG replicate records to kafka source records,
 * eg. 
 * 
 * <pre>
 * new ReplicateRecordConverter()
 *     .schema(kakfaSchema)
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
    /** Kafka schema that defines the converted record schema */
    private Schema schema;

    /**
     * Set the kafka schema that hold the definition of the output kafka
     * source record
     * 
     * @param schema Kafka schema definition for kafka source record
     * 
     * @return ReplicateRecord for chaining calls in builder-type pattern 
     */
    @Override
    public ReplicateRecord schema (Schema schema) {
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
        
        SourceRecord sourceRecord = null;
        
        if (logger.isTraceEnabled()) {
            logger.trace ("Converting: " + record.toJSONString());
        }
        
        switch (record.getDomainRecordType()) {
            case CHANGE_RECORD:
            {
                sourceRecord = convertChangeDataRecord (record);
                break;
            }
            case TRANSACTION_INFO_RECORD:
            {
                sourceRecord = convertTransactionDataRecord (record);
                break;
            }
            case METADATA_RECORD:
            case HEADER_RECORD:
            default:
                break;
        }

        return sourceRecord;
    }
    
    
    /**
     * Convert a single change data record to kafka source record
     * 
     * @param domainRecord A logical change record
     * 
     * @return A kakfa source record
     * @throws Exception Failed to convert a change data record
     */
    private SourceRecord convertChangeDataRecord (DomainRecord domainRecord) 
    throws Exception 
    {
        if (!domainRecord.isChangeRecord()) {
            throw new Exception (
                "Invalid LCR domain record, reason type is " + 
                domainRecord.getDomainRecordType()
            );
        }
        LogicalChangeRecord lcr = (LogicalChangeRecord) domainRecord;

        if (!lcr.hasTableOwner()) {
            throw new Exception (
                "LCR: " + lcr.toJSONString() + " has no owner metadata"
            );
        }

        String topic = schema.name();
        String partKey = topic;

        Struct struct = new Struct (schema);

        /* add row meta data fields */
        struct.put (
            ReplicateSourceTask.METADATA_TRANSACTION_ID_FIELD, 
            lcr.getTransactionId()
        );
        struct.put (
            ReplicateSourceTask.METADATA_CHANGE_TYPE_FIELD,
            lcr.getAction().toString()
        );
        struct.put (
            ReplicateSourceTask.METADATA_CHANGE_ID_FIELD,
            lcr.getId()
        );

        for (ColumnValue cdr : lcr.getColumnValues()) {
            if (cdr != null) {
                Object val = cdr.getValue();

                /* convert LOBs */
                if (cdr.getValue() instanceof SerialBlob) {
                    SerialBlob sb = (SerialBlob) val;
                    val = sb.getBytes(1, (int)sb.length());
                }
                struct.put (cdr.getName(), val);
            }
        }

        return (
            new SourceRecord (
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_NAME_KEY, 
                    partKey
                ),
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_OFFSET_KEY,
                    lcr.getReplicateOffset().toJSONString()
                ),
                topic, 
                schema, 
                struct
            )
        );
    }
    
    /**
     * Convert PLOG transaction data record to Kafka SourceRecord
     * 
     * @param domainRecord PLOG domain record for transaction data
     * 
     * @return record concverted to Kafka source record
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
        
        String topic = schema.name();
        String partKey = topic;

        Struct struct = new Struct (schema);
        
        struct.put ("XID", txr.getId());
        struct.put ("START_SCN", txr.getStartSCN());
        struct.put ("END_SCN", txr.getEndSCN());
        struct.put ("START_TIME", txr.getStartTime());
        struct.put ("END_TIME", txr.getEndTime());
        struct.put ("START_CHANGE_ID", txr.getStartRecordId());
        struct.put ("END_CHANGE_ID", txr.getEndRecordId());
        struct.put ("CHANGE_COUNT", txr.getRecordCount());
        
        /* array of structs to hold counts per replicated schema */
        List<Struct> sarray = new ArrayList<Struct>();
        Map<String, Integer> counts =  txr.getSchemaRecordCounts();
        for (String name : counts.keySet()) {
            Integer count = counts.get (name);
            Struct cstruct = new Struct (
                schema.field("SCHEMA_CHANGE_COUNT_ARRAY")
                      .schema().valueSchema()
            );
            cstruct.put ("SCHEMA_NAME", name);
            cstruct.put ("CHANGE_COUNT", count);
            sarray.add (cstruct);
        }
       
        struct.put ("SCHEMA_CHANGE_COUNT_ARRAY", sarray);
        
        return (
            new SourceRecord (
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_NAME_KEY, 
                    partKey
                ),
                Collections.singletonMap(
                    ReplicateSourceTask.REPLICATE_OFFSET_KEY,
                    txr.getReplicateOffset().toJSONString()
                ),
                topic, 
                schema, 
                struct
            )
        );
    }
}
