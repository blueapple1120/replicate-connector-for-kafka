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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;


import io.confluent.common.config.ConfigException;

import com.dbvisit.replicate.kafkaconnect.util.Version;
import com.dbvisit.replicate.plog.config.PlogConfig;
import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.DomainRecordType;
import com.dbvisit.replicate.plog.domain.ReplicateOffset;
import com.dbvisit.replicate.plog.domain.ReplicateInfo;
import com.dbvisit.replicate.plog.domain.parser.DomainParser;
import com.dbvisit.replicate.plog.domain.parser.LogicalChangeParser;
import com.dbvisit.replicate.plog.domain.parser.MetaDataParser;
import com.dbvisit.replicate.plog.domain.parser.ProxyDomainParser;
import com.dbvisit.replicate.plog.domain.parser.TransactionInfoParser;
import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.file.PlogFileManager;
import com.dbvisit.replicate.plog.format.EntrySubType;
import com.dbvisit.replicate.plog.format.EntryType;
import com.dbvisit.replicate.plog.format.parser.Parser.StreamClosedException;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;
import com.dbvisit.replicate.plog.reader.DomainReader;
import com.dbvisit.replicate.plog.reader.PlogStreamReader;
import com.dbvisit.replicate.plog.reader.criteria.AndCriteria;
import com.dbvisit.replicate.plog.reader.criteria.Criteria;
import com.dbvisit.replicate.plog.reader.criteria.SchemaCriteria;
import com.dbvisit.replicate.plog.reader.criteria.SchemaOffsetCriteria;
import com.dbvisit.replicate.plog.reader.criteria.SystemChangeNumberCriteria;
import com.dbvisit.replicate.plog.reader.criteria.TypeCriteria;
import com.dbvisit.replicate.plog.reader.criteria.TypeOffsetCriteria;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * ReplicateSourceTask is a Kafka Connect SourceTask implementation that reads 
 * from Dbvisit Replicate plogs and generates Kafka Connect records.
 */
public class ReplicateSourceTask extends SourceTask {
    private static final Logger logger = LoggerFactory.getLogger(
        ReplicateSourceTask.class
    );
    
    /** Topic part key */
    public static final String REPLICATE_NAME_KEY = "replicate";
    /** Message offsets key */
    public static final String REPLICATE_OFFSET_KEY = "position";
    /** Transaction ID field for every kafka message */
    public static final String METADATA_TRANSACTION_ID_FIELD = "XID";
    /** Transaction change type field for every kafka message */
    public static final String METADATA_CHANGE_TYPE_FIELD = "TYPE";
    /** Transaction change ID field for every kafka message */
    public static final String METADATA_CHANGE_ID_FIELD = "CHANGE_ID";
    /** Stop flag */
    private AtomicBoolean stop;
    /** Manages PLOG files on disk */
    private PlogFileManager fileManager;
    /** Kakfa source task configuration */
    private ReplicateSourceTaskConfig config;
    /** Identifier to use as prefix for published topics */
    private String topicPrefix;
    /** Identifier of the aggregate transaction info topic */
    private String txInfoTopic;
    /** Cached schema definitions by topic identifier */
    private Map<String, Schema> schemas;
    /** Tracks the SCN values of incoming replicated schemas */
    private Map<String, Long> schemaValidity;
    /** Flag to indicate when to build kafka schema cache */
    private boolean buildSchema = true;
    /** Parsers for domain objects */
    private Map<EntryType, DomainParser[]> domainParsers;
    /** Identify a domain reader as aggregating information */
    private boolean aggregateReader = false;
    /** Criteria for parsing data from PLOGs */
    @SuppressWarnings("rawtypes")
    private Criteria criteria = null;
    /** Criteria for post filtering parsed data from PLOGs, 
     *  used for aggregates
     */
    @SuppressWarnings("rawtypes")
    private Criteria filterCriteria = null;
    
    /** 
     * Return version of replicate source connector for kafka
     * 
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
      return Version.getVersion();
    }
    
    /**
     * Starts up a kafka task to publish records for replicate schema(s) 
     * parsed from a PLOG stream, as configured by source connector
     * 
     * @param props task configuration properties
     */
    @Override
    public void start(Map<String, String> props) {
        try {
            config = new ReplicateSourceTaskConfig (props);
        } catch (ConfigException e) {
            if (logger.isDebugEnabled()) {
                logger.debug ("Cause: ", e);
            }
            
            throw new ConnectException (
                "Couldn't start ReplicateSourceTask due to " + 
                "configuration error",
                e
            );
        }
        
        /* use schema cache to validate DDL changes during replicate */
        schemas = new HashMap<String, Schema>();
        /* cache the last valid from SCNs for each replicated schema */
        schemaValidity = new HashMap<String, Long>();
        
        /* retrieve the JSON for replicate schema objects from the task
         * configuration
         */
        String [] repJSONs = config.getString (
            ReplicateSourceTaskConfig.REPLICATED_CONFIG
        ).split(
            Pattern.quote(
                ReplicateSourceConnector.REPLICATED_GROUP_DELIMTER
            )
        );
        
        List<Map<String, String>> partitions = 
            new ArrayList<>(repJSONs.length);
        
        if (repJSONs == null || repJSONs.length == 0) {
            throw new ConnectException (
                "Invalid configuration: each Replicate Source Task requires " +
                "replicated table details to process"
            );
        }
        
        topicPrefix = config.getString(
            ReplicateSourceConnectorConfig.TOPIC_PREFIX_CONFIG
        );
        
        if (topicPrefix == null) {
            /*  empty prefix for topics */
            topicPrefix = "";
        }
        
        txInfoTopic = config.getString(
            ReplicateSourceConnectorConfig.
            TOPIC_NAME_TRANSACTION_INFO_CONFIG
        );
        
        if (txInfoTopic == null) {
            txInfoTopic = 
                ReplicateSourceConnectorConfig.
                TOPIC_NAME_TRANSACTION_INFO_DEFAULT;
        }
        
        /* if specified, use this as the filter SCN for all replicated 
         * records in PLOG
         */
        Long globalStartSCN = config.getLong(
            ReplicateSourceConnectorConfig.GLOBAL_SCN_COLD_START_CONFIG
        );
        
        long startPlog = -1L;
        long plogUID   = -1L;
        
        /* start offset for each PLOG, not per table but per group */
        final Map <Long, Long> plogOffsets = 
            new HashMap <Long, Long>();
        
        /* start offset for group, as stored in kafka */
        final Map<Long, Long> groupPlogOffsets =
            new HashMap <Long, Long>();
        
        /* skip offset for each table for start PLOGs above */
        final Map <String, ReplicateOffset> skipOffsets = 
            new HashMap <String, ReplicateOffset>();
        
        /* list of replicated objects */
        List <ReplicateInfo> replicated = new LinkedList <ReplicateInfo> ();
        
        /* kafka reader offsets */
        Map<Map<String, String>, Map<String, Object>> offsets = null;
        
        try {
            Map<String, Boolean> repSchemas = 
                new HashMap<String, Boolean> (repJSONs.length);
            
            for (String repJSON : repJSONs) {
                /* maybe one prepared/replicated table per task or multiple
                 * find the first PLOG and start offset of all tables to read
                 */
                ReplicateInfo rep = ReplicateInfo.fromJSONString(repJSON);
                
                replicated.add (rep);
                
                String partKey = topicPrefix + rep.getIdentifier();
                
                partitions.add (
                    Collections.singletonMap (
                        REPLICATE_NAME_KEY, 
                        partKey
                    )
                );
                
                if (startPlog == -1) {
                    plogUID   = rep.getPlogUID();
                    startPlog = plogUID;
                    plogOffsets.put (plogUID, rep.getDataOffset());
                }
                else {
                    plogUID   = rep.getPlogUID();
                    startPlog = startPlog > plogUID
                                ? plogUID 
                                : startPlog;
                    
                    if (!plogOffsets.containsKey (plogUID) || 
                        rep.getDataOffset() < plogOffsets.get (plogUID)) 
                    {
                        /* lower offset found */
                        plogOffsets.put (plogUID, rep.getDataOffset());
                    }
                }
                
                String repId = rep.getIdentifier();
                
                repSchemas.put (repId, true);
                
                if (rep.isAggregate()) {
                    aggregateReader = true;
                }
            }
            
            initDomainParsers(repSchemas);
            
            /* fetch the offsets for each partition 
             * 
             * when task are reconfigured check which PLOG/offset to use
             * as start, either the ones provided by monitor or the
             * ones stored by kafka
             * 
             * next determine the skip offset criteria for each partition
             * or table to prevent reader from pushing duplicates */
            offsets = context.offsetStorageReader().offsets(partitions);
            
            long groupPlogUID = -1L;
            
            for (ReplicateInfo repInfo : replicated) {
                String partKey = topicPrefix + repInfo.getIdentifier();
                Map<String, Object> offset = 
                    offsets == null 
                    ? null 
                    : offsets.get(
                        Collections.singletonMap (
                            REPLICATE_NAME_KEY, 
                            partKey
                        )
                    );
                
                if (offset != null && 
                    offset.containsKey (REPLICATE_OFFSET_KEY))
                {
                    /* the monitor thread provide offset of first metadata
                     * record for each table, kafka provides the last offset
                     * where data written to topic was read from, this is
                     * the skip offset.
                     *
                     * The monitor thread will always restart at oldest
                     * PLOG that kafka has last processed for all tables
                     * in group
                     */
                    Object storedOffset = offset.get (REPLICATE_OFFSET_KEY);
                    
                    /* report non-backwards compatible offset */
                    if (storedOffset != null && storedOffset instanceof Long) {
                        long oldOffset = (Long)storedOffset;
                        throw new Exception (
                            "Deprecated composite 64-bit replicate offset " + 
                            "value found as kafka offset value in "         +
                            "topic: " + partKey + ". Unable to continue "   +
                            "publishing to this topic, please change "      +     
                            "topic prefix: " + topicPrefix + " in "         +
                            "configuration and restart at PLOG: "           +
                            (oldOffset >> 32) + " and offset: "             +
                            (oldOffset & 0xFFFFFFFF)
                        );
                    }
                    
                    if (storedOffset == null || 
                        false == storedOffset instanceof String) {
                        throw new Exception (
                            "Invalid stored offset found for topic: " +
                            partKey + ", reason: " +
                            (
                                storedOffset == null
                                ? "null offset" 
                                : "not a replicate offset JSON string"
                            )
                        );
                    }
                    
                    String offsetJSON = (String)storedOffset;
                        
                    ReplicateOffset repOffset = null;
                    
                    try {
                        repOffset = ReplicateOffset.fromJSONString(offsetJSON);
                    }
                    catch (Exception e) {
                        throw new Exception (
                            "Invalid replicate offset found for topic: " +
                            partKey + ", reason: " + e.getMessage()
                        );
                    }
                    
                    logger.debug (
                        "Offset JSON - " +
                        repInfo.getIdentifier() + ":" + offsetJSON
                    );
                    
                    long uid = repOffset.getPlogUID();
                    long off = repOffset.getPlogOffset();
                    
                    /* if kafka writer tasks is behind monitoring, restart at
                     * PLOG for last committed kafka message
                     */
                    if (uid < startPlog) {
                        /* reading behind monitoring for this task and
                         * group of tables processed, redo PLOGs */
                        startPlog = uid;
                        
                        plogOffsets.remove (startPlog);
                        /* re-read */
                        plogOffsets.put (startPlog, 0L);
                    }
                    
                    if (groupPlogUID == -1L || uid < groupPlogUID) {
                        groupPlogUID = uid;
                    }
                    
                    if (!groupPlogOffsets.containsKey (groupPlogUID) ||
                        groupPlogOffsets.get (groupPlogUID) > off) 
                    {
                        groupPlogOffsets.put (groupPlogUID, off);
                    }
                    
                    /* skip PLOG and offset for this table, not every PLOG
                     * will contain LCRs for all tables in the group, however
                     * only one reader is used per task and it should only
                     * start processing LCRs for this table where kafka 
                     * task stopped reading it last, eg. a different task may
                     * have been processing this table last time */
                    skipOffsets.put (repInfo.getIdentifier(), repOffset);
                    
                    logger.info (
                        "Kafka offset retrieved for "                 + 
                        "schema: " + repInfo.getIdentifier()          + " " +
                        "PLOG: "   + PlogFile.getFileNameFromUID(uid) + " " + 
                        "offset: " + off
                    );
                }
            }

            if (groupPlogUID != -1 && groupPlogUID > startPlog) {
                /* start at lowest sequence of read PLOG committed to kafka
                 * for this group of replicated schemas
                 */
                startPlog = groupPlogUID;
                plogOffsets.put (startPlog, 0L);
            }

            logger.info (
                "Processing starting at PLOG: "        + 
                PlogFile.getFileNameFromUID(startPlog) + " " + 
                "at file offset: " + 
                (
                    groupPlogOffsets.containsKey (startPlog) 
                    ? groupPlogOffsets.get (startPlog)
                    : plogOffsets.get (startPlog)
                ) + " " +
                "schemas: " + repSchemas.keySet().toString()
            );
            
            /* initialise parse criteria */
            initParseCriteria(repSchemas, skipOffsets, globalStartSCN);
            
            /* intialise post-parse filter criteria */
            initFilterCriteria(skipOffsets, globalStartSCN);
            
        } catch (Exception e) {
            throw new ConnectException (
                "Failed to configure Replicate Source Task, reason " +
                e.getMessage()
            );
        }
        
        if (fileManager == null) {
            /* setup and configure file manager */
            try {
                /* each task tracks their own PLOGs, start scanning at 
                 * first PLOG 
                 */
                fileManager = new PlogFileManager (
                    new PlogConfig(props),
                    startPlog
                );
                
                /* block until PLOG arrives */
                fileManager.scan ();
                PlogFile plog = fileManager.getPlog();
                
                /* setup reader for new PLOG upon task start */
                PlogStreamReader reader = plog.getReader();
                DomainReader domainReader = reader.getDomainReader();
                
                /* set if it's reading and aggregating */
                domainReader.setIsAggregateReader(aggregateReader);
                
                /* set parse filter criteria */
                domainReader.setParseCriteria(criteria);
                
                /* set post filter criteria */
                domainReader.setFilterCriteria(filterCriteria);
                
                /* for kafka we need to merge multi-part LCRs and only publish
                 * one message
                 */
                domainReader.enableMultiPartMerging();
                
                /* set parsers to handle converting PLOG entries to domain 
                 * records by type
                 */
                domainReader.setDomainParsers(domainParsers);
                
                /* set types to persist after parsing to domain records */
                domainReader.setPersistCriteria(
                    new TypeCriteria<EntrySubType> (persistentTypes)
                );
                
                /* set data flush/batch size */
                String flushConfigProp = 
                    ReplicateSourceConnectorConfig.PLOG_DATA_FLUSH_CONFIG;
                    
                reader.setFlushSize (config.getInt(flushConfigProp));
                
                long startOffset = plogOffsets.get (startPlog);

                reader.forward (startOffset);
                
                buildSchema = true;
                
                /* build static transaction info schema */
                buildTransactionInfoSchema ();
            }
            catch (Exception e) {
                throw new ConnectException (
                    "Could not initialise PLOG file manager, reason: " +
                    e.getMessage()
                );
            }
        }
        
        stop = new AtomicBoolean(false);
    }  

    /**
     * Stop processing PLOGs by closing the file manager, all open
     * PLOGS streams and stopping this task
     * 
     * @throws ConnectException if any fatal error occur
     */
    @Override
    public void stop() throws ConnectException {
        fileManager.close();
        
        if (stop != null) {
            stop.set(true);
        }
    }
    
    /** Poll PLOG stream reader to flush and emit new records to kafka,
     *  blocks when no new data is available.
     *  
     *  @return list of source records or null when done or no more data
     *  
     *  @throws InterruptedException when interrupted.
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        while (!stop.get()) {
            try {
                PlogFile plog = null;
                PlogStreamReader reader = null;
                List<SourceRecord> records = null;

                if (!fileManager.isActive()) {
                    /* should be scanning already */
                    throw new ConnectException (
                        "FileManager is not active"
                    );
                }
                
                plog = fileManager.getPlog();
                
                if (plog == null || !plog.canUse()) {
                    /* scan for next PLOG */
                    fileManager.scan ();
                    plog = fileManager.getPlog();
                    
                    /* setup reader for new PLOG */
                    reader = plog.getReader();
                    
                    DomainReader domainReader = reader.getDomainReader();
                    
                    /* set if it's reading and aggregating */
                    domainReader.setIsAggregateReader(aggregateReader);
                    
                    /* set parse criteria */
                    domainReader.setParseCriteria(criteria);
                    
                    /* set post filter criteria */
                    domainReader.setFilterCriteria(filterCriteria);
                    
                    /* for kafka we need to merge multi-part LCRs and only 
                     * publish one message
                     */
                    domainReader.enableMultiPartMerging();
                    
                    /* set default domain parsers */
                    domainReader.setDomainParsers(domainParsers);
                    
                    /* set types to persist */
                    domainReader.setPersistCriteria(
                        new TypeCriteria<EntrySubType> (persistentTypes)
                    );
                    
                    /* always flush transaction info at the end of file */
                    domainReader.enableFlushLastTransactions();
                    
                    /* set data flush/batch size */
                    String flushConfigProp = 
                        ReplicateSourceConnectorConfig.PLOG_DATA_FLUSH_CONFIG;
                    
                    reader.setFlushSize (config.getInt(flushConfigProp));

                    /* build schema when we start */
                    buildSchema = true;
                    
                    logger.info ("Processing PLOG: " + plog.getFileName());
                }

                /* either new PLOG or still busy reading */
                reader = plog.getReader();
                
                /* read a batch of PLOG entries from stream */
                while (!reader.isDone() && !reader.canFlush()) {
                    reader.read();
                }
                
                /* if needed, update schema definitions */
                if (buildSchema || plog.hasUpdatedSchema()) {
                    buildSchemas (plog);
                    buildSchema = false;
                    plog.setUpdatedSchema(false);
                }

                if (reader.canFlush()) {
                    records = convertRecords (reader.flush());

                    if (records != null && records.size() > 0) {
                        logger.info (
                            "Publishing " + records.size() + " kafka records"
                        );
                    }
                }
                
                return records;
            }
            catch (InterruptedException e) {
                /* interrupted, return null, normal shutdown */
                logger.warn ("Forcefully interrupted");
                
                return null;
            }
            catch (StreamClosedException se) {
                /* this okay, we have been forced to shutdown */
                logger.warn (
                    "Shutting down PLOG processor "  + 
                    Thread.currentThread().getName() + ", " +
                    "reason: stream closed"
                );
                
                return null;
            }
            catch (Exception e) {
                boolean hasMsg = (
                    e.getMessage() == null
                    ? false
                    : true
                );
                
                String err = (
                    hasMsg 
                    ? e.getMessage() 
                    :"an internal processing error has occurred"
                );
                
                logger.error (
                    "An unrecoverable error has occurred and must be " +
                    "resolved before restarting. " + err
                );
                
                if (logger.isDebugEnabled() || !hasMsg) {
                    logger.debug ("Cause: ", e);
                }
                
                /* halt task */
                throw new InterruptedException ();
            }
        }
        
        return null;
    }
  
    /** 
     * Convert domain to kafka records
     * 
     * @param batch A batch of domain records to process
     * 
     * @return A batch of kafka records
     * @throws Exception Failed to convert records
     */
    private List<SourceRecord> convertRecords (List<DomainRecord> batch)
    throws Exception 
    {
        List<SourceRecord> records = new ArrayList<SourceRecord>();
        
        for (DomainRecord domainRecord : batch) {
            String kafkaSchema = toKafkaTopicSchema(
                domainRecord.isTransactionInfoRecord()
                ? txInfoTopic
                : domainRecord.getRecordSchema()
            );

            if (!schemas.containsKey(kafkaSchema)) {
                logger.warn (
                    "No Schema found for kafka schema: " + kafkaSchema + ". " +
                    "This probably means it was not prepared and configured " +
                    "for replication"
                );
                /* skip it */
                return null;
            }

            /* convert PLOG domain record to kafka source record */
            SourceRecord sourceRecord = 
                new ReplicateRecordConverter()
                    .schema (schemas.get (kafkaSchema))
                    .record (domainRecord)
                    .convert();
            
            if (sourceRecord != null) {
                records.add (sourceRecord);
            }
        }
        
        batch.clear ();
        
        return records;
    }
    

    /**
     * Convert all schema meta data registered in PLOG to kafka schemas
     * 
     * @param plog PLOG file with cached schema meta data
     * @throws Exception Failed to build schemas
     */
    private void buildSchemas (PlogFile plog)
    throws Exception {
        Object[] schemaNames = 
            plog.getSchemas().keySet().toArray();
        
        for (Object schemaName : schemaNames) {
            /* build schema per topic prefix to prevent schema name clashes */
            String kafkaSchema = toKafkaTopicSchema ((String)schemaName);
            
            DDLMetaData metadata = plog.getSchemas().get (schemaName);
            
            /* schema per topic */
            if (!schemas.containsKey (kafkaSchema)) {
                /* first version */
                Schema schema = new ReplicateSchemaConverter()
                    .topicName(kafkaSchema)
                    .metadata(metadata)
                    .convert();
                
                schemas.put (kafkaSchema, schema);
                schemaValidity.put(kafkaSchema, metadata.getValidSinceSCN());
            }
            else {
                if (schemaValidity.containsKey (kafkaSchema)) {
                    /* compare SCN validity */
                    Long lastSCN = schemaValidity.get (kafkaSchema);
                    
                    if (metadata.getValidSinceSCN() > lastSCN) {
                        /* have an update */
                        Schema prevSchema = schemas.get (kafkaSchema);
                        
                        /* create new version of schema */
                        Schema schema = new ReplicateSchemaConverter()
                            .topicName(kafkaSchema)
                            .schema(prevSchema)
                            .metadata(metadata)
                            .update();      
                                
                        schemas.put (kafkaSchema, schema);
                        schemaValidity.put(
                            kafkaSchema,
                            metadata.getValidSinceSCN()
                        );
                    }
                }
            }
        }
    }
    
    /**
     * Prepend topix prefix to schema name to create kafka topic name
     * 
     * @param schemaName Schema name of topic
     * 
     * @return Kafka topic name
     */
    private String toKafkaTopicSchema (String schemaName) {
        return topicPrefix + schemaName;
    }
    
    /**
     * Create pre-defined transaction data record schema for Kafka
     */
    private void buildTransactionInfoSchema () {
        String kafkaSchema = toKafkaTopicSchema(txInfoTopic);
        
        SchemaBuilder builder = SchemaBuilder.struct().name(kafkaSchema);

        if (!schemas.containsKey (txInfoTopic)) {
            /* static topic */
            for (String field : txMetaFields.keySet()) {
                Schema schema = txMetaFields.get (field);
                
                builder.field (field, schema);
            }
            
            /* schema change count array field */
            SchemaBuilder sbuilder = 
                SchemaBuilder.struct().name ("SCHEMA_CHANGE_COUNT");
                
            sbuilder.field (
                "SCHEMA_NAME",
                Schema.STRING_SCHEMA
            );
            
            sbuilder.field (
                "CHANGE_COUNT",
                Schema.INT32_SCHEMA
            );
                
            builder.field (
                "SCHEMA_CHANGE_COUNT_ARRAY",
                SchemaBuilder.array(sbuilder.build()).build()
            );
            
            schemas.put (kafkaSchema, builder.build());
        }
    }
    
    /** 
     * Initialises the domain's parse criteria
     * 
     * @param schemas        The schemas to parse
     * @param schemaOffsets  The data offsets for each schema to parse
     * @param globalStartSCN The SCN to load/process all data from
     */
    private void initParseCriteria (
        final Map <String, Boolean> schemas,
        final Map <String, ReplicateOffset> schemaOffsets,
        final Long globalStartSCN
    ) {
        SchemaCriteria<EntrySubType> schemaCriteria          = null;
        SystemChangeNumberCriteria<EntrySubType> scnCriteria = null;
        
        if (schemas.size() > 0) {
            schemaCriteria = new SchemaCriteria<EntrySubType> (schemas);
        }
        
        /* define criteria for reader */
        if (schemaOffsets.size() > 0) {
            /* data have already been published to kafka, skip to the
             * correct stream offset before publishing new data
             */
            for (String sid : schemaOffsets.keySet()) {
                ReplicateOffset repOff = schemaOffsets.get (sid);
                long uid = repOff.getPlogUID();
                long off = repOff.getPlogOffset();
                
                logger.debug (
                    "Last published messages for topic: " + 
                    topicPrefix + sid + " in PLOG: "      + 
                    PlogFile.getFileNameFromUID(uid)      + 
                    " at offset: " + off
                );
            }
            
            SchemaOffsetCriteria<EntrySubType> schemaOffsetCriteria = 
                new SchemaOffsetCriteria<EntrySubType> (schemaOffsets);
            
            if (schemaCriteria != null) {
                criteria = new AndCriteria<EntrySubType> (
                    schemaCriteria,
                    schemaOffsetCriteria
                );
            }
            else {
                criteria = schemaOffsetCriteria;
            }
        }
        else {
            /* cold start, no data published to kafka, yet */
            if (globalStartSCN != null) {
                scnCriteria = 
                    new SystemChangeNumberCriteria<>(globalStartSCN);
            }
            if (schemaCriteria != null && scnCriteria != null) {
                /* each topic handles a group of replicated schemas 
                 * and global SCN filter
                 */
                criteria = new AndCriteria<EntrySubType> (
                    schemaCriteria,
                    scnCriteria
                );
            }
            else if (schemaCriteria != null && scnCriteria == null) {
                criteria = schemaCriteria;
            }
            else {
                criteria = scnCriteria;
            }
        }
    }
    
    /**
     * Initialises the filter criteria for filtering domain records after
     * parsing, this is meant for aggregate records
     * 
     * @param schemaOffsets  The data offsets for each schema to parse
     * @param globalStartSCN The SCN to load/process all data from
     */
    @SuppressWarnings("serial")
    private void initFilterCriteria (
        final Map <String, ReplicateOffset> schemaOffsets,
        final Long globalStartSCN
    ) {
        /* only applied for aggregates */
        if (aggregateReader) {
            TypeOffsetCriteria<DomainRecordType> typeOffsetCriteria  = null;
            SystemChangeNumberCriteria<DomainRecordType> scnCriteria = null;
            
            /* use skip offset for aggregate TX info topic to apply post, not
             * parse filter
             */
            if (schemaOffsets.containsKey(txInfoTopic)) {
                typeOffsetCriteria = 
                    new TypeOffsetCriteria<DomainRecordType>(
                        new HashMap<DomainRecordType, ReplicateOffset>() {{
                            put (
                                DomainRecordType.TRANSACTION_INFO_RECORD,
                                schemaOffsets.get(txInfoTopic)
                            );
                            /* do not filter change records */
                            put (
                                DomainRecordType.CHANGE_RECORD,
                                new ReplicateOffset(0L, 0L) 
                            );
                        }}
                    );
            }
            
            if (globalStartSCN != null) {
                scnCriteria = 
                    new SystemChangeNumberCriteria<DomainRecordType> (
                        globalStartSCN
                    );
            }
            
            if (typeOffsetCriteria != null && scnCriteria != null) {
                filterCriteria = new AndCriteria<DomainRecordType> (
                    typeOffsetCriteria,
                    scnCriteria
                );
            }
            else if (typeOffsetCriteria != null && scnCriteria == null) {
                filterCriteria = typeOffsetCriteria;
            }
            else {
                filterCriteria = scnCriteria;
            }
        }
    }
    
    /**
     * Initialises the domain parsers for prepared replicated schemas
     * 
     * @param repSchemas schemas to process
     */
    private void initDomainParsers (Map<String, Boolean> repSchemas) {
        /* domain parsers for parsing all known data types */
        domainParsers = new HashMap<EntryType, DomainParser[]>();
        
        if (aggregateReader) {
            /* single transaction info parser for aggregating */
            TransactionInfoParser txParser = new TransactionInfoParser();
            
            DomainParser[] dataParsers = null;
            if (repSchemas.size() == 1 && 
                repSchemas.containsKey (txInfoTopic)) 
            {
                /* only one schema and it's the aggregate tx info, no need
                 * to parse any data, only aggregate
                 */
                dataParsers = new DomainParser[] {
                    txParser
                };
            }
            else {
                dataParsers = new DomainParser[] {
                    new LogicalChangeParser(),
                    txParser
                };
                /* need meta data for LCR parsing */
                domainParsers.put (
                    EntryType.ETYPE_METADATA,
                    new DomainParser[] {
                        new MetaDataParser()
                    }
                );
            }
            domainParsers.put (
                EntryType.ETYPE_LCR_DATA,
                dataParsers
                
            );
            domainParsers.put (
                EntryType.ETYPE_TRANSACTIONS, 
                new DomainParser[] { 
                    txParser
                }
            );
        }
        else {
            /* parse schema meta data and data records only, including
             * ones from included LOAD PLOGs */ 
            domainParsers.put (
                EntryType.ETYPE_METADATA,
                new DomainParser[] {
                    new MetaDataParser()
                }
            );
            domainParsers.put (
                EntryType.ETYPE_LCR_DATA,
                new DomainParser[] {
                    new LogicalChangeParser()
                }
            );
        }
        
        domainParsers.put (
            EntryType.ETYPE_LCR_PLOG,
            new DomainParser[] { 
                new ProxyDomainParser() 
            }
        );
    }
    
    /** Types to persist to cache after parsing to PLOG domain records */
    @SuppressWarnings("serial")
    private final Map<EntrySubType, Boolean> persistentTypes = 
        new HashMap<EntrySubType, Boolean> () {{
            put (EntrySubType.ESTYPE_LCR_INSERT,     true);
            put (EntrySubType.ESTYPE_LCR_UPDATE,     true);
            put (EntrySubType.ESTYPE_LCR_DELETE,     true);
            put (EntrySubType.ESTYPE_LCR_LOB_WRITE,  true);
            put (EntrySubType.ESTYPE_LCR_LOB_ERASE,  true);
            put (EntrySubType.ESTYPE_LCR_LOB_TRIM,   true);
            put (EntrySubType.ESTYPE_LCR_PLOG_IFILE, true);
    }};
    
    /** Transaction data record field definition */
    @SuppressWarnings("serial")
    private final Map<String, Schema> txMetaFields =
        new LinkedHashMap<String, Schema>() {{
            put ("XID",             Schema.STRING_SCHEMA);
            put ("START_SCN",       Schema.INT64_SCHEMA);
            put ("END_SCN",         Schema.INT64_SCHEMA);
            put ("START_TIME",      Timestamp.builder().build());
            put ("END_TIME",        Timestamp.builder().build());
            put ("START_CHANGE_ID", Schema.INT64_SCHEMA);
            put ("END_CHANGE_ID",   Schema.INT64_SCHEMA);
            put ("CHANGE_COUNT",    Schema.INT32_SCHEMA);
    }};
}

