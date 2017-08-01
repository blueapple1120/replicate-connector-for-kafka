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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.file.PlogFileManager;
import com.dbvisit.replicate.plog.format.EntrySubType;
import com.dbvisit.replicate.plog.format.EntryType;
import com.dbvisit.replicate.plog.format.parser.FormatParser.StreamClosedException;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;
import com.dbvisit.replicate.plog.reader.DomainReader;
import com.dbvisit.replicate.plog.reader.DomainReader.DomainReaderBuilder;
import com.dbvisit.replicate.plog.reader.PlogStreamReader;
import com.dbvisit.replicate.plog.reader.criteria.InternalDDLFilterCriteria;
import com.dbvisit.replicate.plog.reader.criteria.TypeCriteria;
import com.dbvisit.replicate.kafkaconnect.util.ReplicateInfoTopic;
import com.dbvisit.replicate.plog.config.PlogConfig;
import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.DomainRecordType;
import com.dbvisit.replicate.plog.domain.MetaDataRecord;
import com.dbvisit.replicate.plog.domain.ReplicateInfo;
import com.dbvisit.replicate.plog.domain.parser.DomainParser;
import com.dbvisit.replicate.plog.domain.parser.MetaDataParser;
import com.dbvisit.replicate.plog.domain.parser.ProxyDomainParser;

/** 
 * Monitor PLOG location for arrival of meta data LCRs, emit these
 * as serialized ReplicateSchema to tasks
 */
public final class PlogMonitorThread extends Thread {
    /** Default maximum timeout in milliseconds */
    private static final int MAX_TIMEOUT = 10000;
    
    private static final Logger logger = LoggerFactory.getLogger (
        PlogMonitorThread.class
    );
    /** File manager to scan for PLOGs */
    private final PlogFileManager fileManager;
    /** The context to use for connector */
    private final ConnectorContext context;
    /** Shutdown latch */
    private final CountDownLatch shutdownLatch;
    /** Handler for persisting replicate info records to Kafka topic */
    private final ReplicateInfoTopic infoTopic;
    /** Persistent catalog of replicate info for task configuration */
    private final Map <String, ReplicateInfo> catalog;
    /** Internal transaction info topic */
    private final String transactionTopicName;
    /** Whether or not the connector will be publishing transaction info */
    private final boolean publishTransactions;
    /** Flag to control whether or not the monitor has an updated catalog
     *  for updating the configuration of tasks */
    private final AtomicBoolean updateTasks;
    /** Flag to indicate whether or not this run will start tasks */
    private final AtomicBoolean startTasks;
    /** Handle interrupt and shutdown */
    private final ShutdownHook shutdownHook;
    
    /** 
     * Create and configure the components needed to compose and build a
     * valid PLOG monitor thread
     */
    public static class PlogMonitorThreadComposer {
        private PlogFileManager fileManager;
        private ConnectorContext context;
        private ReplicateInfoTopic infoTopic;
        private Map <String, ReplicateInfo> catalog;
        private String transactionTopicName;
        private boolean publishTransactions;
        
        /**
         * Set the source connector context to use for PLOG monitor thread
         * 
         * @param context source connector context
         * @return this composer
         */
        public PlogMonitorThreadComposer context(
            final ConnectorContext context
        ) {
            this.context = context;
            
            return this;
        }
        
        /**
         * Configure the components needed for PLOG monitor thread
         * 
         * @param props the source connector configuration properties
         * 
         * @return this composer
         * @throws Exception for any configuration errors
         */
        public PlogMonitorThreadComposer configure(
            final Map<String, String> props
        ) throws Exception {
            if (props == null || props.isEmpty()) {
                throw new Exception (
                    "No configuration properties provided, cannot configure " +
                    "PLOG monitor"
                );
            }
            
            /* configure catalog before file manager */
            configureReplicateInfoTopic(props);
            configureFileManager(props);
            configureTransactionTopic(props);
            
            return this;
        }
        
        /**
         * Configure the replicate info topic for reading/writing to the
         * internal replicate catalog topic required by PLOG monitor
         * thread
         * 
         * @param props the source connector configuration properties
         * @throws Exception for any replicate info topic configuration errors
         */
        protected void configureReplicateInfoTopic (
            final Map<String, String> props
        ) throws Exception 
        {
            /* configure and build replicate info topic */
            infoTopic = ReplicateInfoTopic.composer()
                        .configure(props)
                        .build();
            
            /* configure catalog from latest messages in replicate info topic */
            catalog = infoTopic.read();
            
            if (!catalog.isEmpty()) {
                logger.info (
                    "Restarting PLOG monitor with catalog: " + 
                    catalog.toString()
                );
            }
        }
        
        /**
         * Configure the properties needed for publishing transaction 
         * catalog entries for tasks
         * 
         * @param props the source connector configuration properties
         * @throws Exception for any configuration errors
         */
        protected void configureTransactionTopic (
            final Map<String, String> props
        ) throws Exception
        {
            ReplicateSourceConnectorConfig config;
            try {
                config = new ReplicateSourceConnectorConfig (props);
            } catch (Exception e) {
                throw new Exception (
                    "Failed crearing source connector configuration, reason: " +
                    e.getMessage(),
                    e
                );
            }
            
            /* configure whether or not it should monitor transaction metadata */
            publishTransactions = config.getBoolean(
                ReplicateSourceConnectorConfig.CONNECTOR_PUBLISH_TX_INFO_CONFIG
            );

            logger.debug (
                 "Configured for monitoring transaction info: " + 
                 publishTransactions
            );

            if (publishTransactions) {
                transactionTopicName = config.getString(
                    ReplicateSourceConnectorConfig
                    .TOPIC_NAME_TRANSACTION_INFO_CONFIG
                );
            }
        }
        
        /**
         * Configure the PLOG file manager required to produce PLOG files 
         * to be monitored for replicate info offset records
         * 
         * @param props the source connector configuration properties
         * @throws Exception for any configuration errors
         */
        protected void configureFileManager (final Map<String, String> props) 
        throws Exception {
            if (catalog == null) {
                throw new Exception (
                    "Unable to configure PLOG file manager, reason: " +
                    "invalid replicate schema catalog"
                );
            }
            fileManager = new PlogFileManager (
                new PlogConfig(props),
                readerBuilder()
            );
            
            /* Configure file manager from catalog */
            if (!catalog.isEmpty()) {
                long restartUID = -1;
                for (ReplicateInfo repCache : catalog.values()) {
                    long plogUID = repCache.getPlogUID();
                    if (restartUID == -1L || plogUID > restartUID) {
                        restartUID = plogUID;;
                    }
                }
                if (restartUID != -1) {
                    logger.debug (
                        "Configure monitor to start after last cache entry: " +
                        PlogFile.getFileNameFromUID(restartUID)
                    );
                    fileManager.startAfter(restartUID);
                }
            }
        }
        
        /**
         * Build the domain reader for PLOG file manager to use for 
         * all reading all PLOGs
         * 
         * @return domain reader builder for file manager
         */
        private DomainReaderBuilder readerBuilder() {
            return DomainReader.builder()
                .parseCriteria(new TypeCriteria<EntrySubType> (subtypes))
                .persistCriteria(new TypeCriteria<EntrySubType> (persistent))
                .filterCriteria(new InternalDDLFilterCriteria<DomainRecordType>())
                .domainParsers(domainParsers);
        }
        
        /**
         * Validate the components required prior to building the PLOG
         * monitor thread
         * 
         * @throws Exception for any validation errors
         */
        private void validate() throws Exception {
            if (fileManager == null) {
                throw new Exception(
                    "Unable to build PLOG replicate monitor, reason: "   +
                    "PLOG file manager has not been configured"
                );
            }
            if (context == null) {
                throw new Exception(
                    "Unable to build PLOG replicate monitor, reason: "   +
                    "no source connector context provided"
                );                
            }
            if (infoTopic == null || catalog == null) {
                throw new Exception(
                    "Unable to build PLOG replicate monitor, reason: "   +
                    "replicate info topic has not been configured"
                );               
            }
            if (publishTransactions && (
                    transactionTopicName == null 
                 || transactionTopicName.length() == 0)) {
                throw new Exception(
                    "Unable to build PLOG replicate monitor, reason: "   +
                    "publishing transactions are enabled, but no topic " +
                    "name for transactions configured"
                );
            }
        }
        
        /**
         * Build a valid PLOG monitor from its required components
         * 
         * @return valid PLOG monitor
         * @throws Exception for any validation errors
         */
        public PlogMonitorThread build() throws Exception {
            validate();
            
            return new PlogMonitorThread(
                context,
                fileManager,
                infoTopic,
                catalog,
                transactionTopicName,
                publishTransactions
            );
        }
        
        /** Parse only DDL JSON entry records for schema definition, including 
         *  ones from included LOAD PLOG files, and footer record to end PLOG
         */
        @SuppressWarnings("serial")
        private final Map<EntrySubType, Boolean> subtypes = 
            new HashMap<EntrySubType, Boolean> () {{
                put (EntrySubType.ESTYPE_DDL_JSON, true);
                put (EntrySubType.ESTYPE_LCR_PLOG_IFILE, true);
                put (EntrySubType.ESTYPE_FOOTER, true);
        }};
        
        /** Persist only schema meta data in JSON format */
        @SuppressWarnings("serial")
        private final Map<EntrySubType, Boolean> persistent = 
            new HashMap<EntrySubType, Boolean> () {{
                put (EntrySubType.ESTYPE_DDL_JSON, true);
                put (EntrySubType.ESTYPE_LCR_PLOG_IFILE, true);
        }};
        
        /** Domain parsers include DDL JSON parser and proxy parser for
         *  DDL JSON in included PLOGs
         */
        @SuppressWarnings("serial")
        private final Map<EntryType, DomainParser[]> domainParsers = 
            new HashMap<EntryType, DomainParser[]> () {{
                put (
                    EntryType.ETYPE_METADATA, 
                    new DomainParser[] { 
                        new MetaDataParser()   
                    }
                );
                put (
                    EntryType.ETYPE_LCR_PLOG,
                    new DomainParser[] { 
                        new ProxyDomainParser() 
                    }
                );
        }};
    }
    
    /**
     * Create PLOG monitoring thread
     * 
     * @param context Connector context to use for monitoring thread to
     *                request task reconfiguration
     * @param fileManager the PLOG file manager to use for producing PLOGs
     * @param infoTopic the replicate info topic reader/writer
     * @param catalog the persistent replicate offset catalog read from topic
     * @param transactionTopicName name of transaction info topic to use
     * @param publishTransactions whether or not to publish transaction messages
     * 
     * @throws Exception when any startup error occurs
     */
    public PlogMonitorThread (
        final ConnectorContext context, 
        final PlogFileManager fileManager,
        final ReplicateInfoTopic infoTopic,
        final Map <String, ReplicateInfo> catalog,
        final String transactionTopicName,
        final boolean publishTransactions
    ) throws Exception 
    {
        this.context     = context;
        this.fileManager = fileManager;
        this.infoTopic   = infoTopic;
        this.catalog     = catalog;
        this.transactionTopicName = transactionTopicName;
        this.publishTransactions  = publishTransactions;
        this.shutdownLatch = new CountDownLatch(1);
        this.updateTasks   = new AtomicBoolean(false);
        this.startTasks    = new AtomicBoolean(catalog.isEmpty());
        
        setName ("PLOG monitor thread");
        
        /* add shutdown hook */
        shutdownHook = new ShutdownHook();
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
    
    /**
     * Create the composer to use for composing and building a valid
     * PLOG monitor thread for the Source connector
     * 
     * @return configured and valid PLOG monitor
     */
    public static PlogMonitorThreadComposer composer() {
        return new PlogMonitorThreadComposer();
    }
    
    /**
     * Monitor replicated schemas in PLOG stream 
     */
    @Override
    public void run() {
        logger.debug ("Running PLOG monitor");
        
        PlogFile plog = null;
        ReplicateInfo rep = null;
        
        fileManager.setForceInterrupt();
        
        while (shutdownLatch.getCount() > 0) {
            try {
                fileManager.scan();
                plog = fileManager.getPlog();
                
                if (plog == null || !plog.canUse()) {
                    throw new Exception (
                        "Invalid PLOG file found, " + (plog == null 
                        ? "no valid plog found" 
                        : plog.getFileName() + " is not a valid plog")
                    );
                }
                
                logger.info ("Monitoring PLOG: " + plog.getFileName());
            }
            catch (Exception e) {
                shutdown();
                if (!(e instanceof InterruptedException)) {
                    logger.error (
                        "An unrecoverable error has occurred, error: " +
                        e.getMessage() + ", shutting down PLOG monitor"
                    );
                    
                    if (logger.isDebugEnabled()) {
                        logger.debug ("Cause: ", e);
                    }
                    context.raiseError(e);
                }
            }
            
            try {
                if (shutdownLatch.getCount() == 0) {
                    /* shutdown was already triggered */
                    throw new InterruptedException("Shutdown in progress");
                }
                /* configured to only read JSON DDL records, these are emitted 
                 * when new table is added to replication
                 */
                PlogStreamReader reader = plog.getReader();

                /* flush after record, could change in future */
                reader.setFlushSize(1);
            
                boolean updateCatalog = false;
                while (!reader.isDone()) {
                    while (!reader.isDone() && !reader.canFlush()) {
                        reader.read();
                    }

                    /* we've found an record with meta data, the PLOG
                     * will now contain schema information, append
                     * it to our cache, if it doesn't exist
                     */
                    List<DomainRecord> domainRecords = reader.flush();

                    for (DomainRecord domainRecord : domainRecords) {
                        if (!domainRecord.isMetaDataRecord()) {
                            throw new Exception (
                                "PLOG Monitor can only parse meta data records, " + 
                                "found: " + domainRecord.getDomainRecordType()
                            );
                        }
                        
                        MetaDataRecord mdr = (MetaDataRecord)domainRecord;
                        
                        /* filter internal and recycle bin DDL meta data */
                        if (!mdr.getMetaData().fromRecycleBin())
                        {
                            /* each MDR has DDL meta data */
                            DDLMetaData ddl = mdr.getMetaData();
                            String schemaName = ddl.getSchemataName();
                            
                            rep = new ReplicateInfo ();

                            rep.setPlogUID(plog.getUID());
                            
                            /* start offset of JSON entry record */
                            long startOffset = 0L;
                            
                            /* if paused the record was emitted by proxy
                             * parser and need to have the offset of the
                             * start of its parent IFILE record as marked 
                             * for rewind operation
                             */
                            if (reader.isPaused()) {
                                startOffset = reader.getOffsetMarker();
                            }
                            else {
                                startOffset =
                                    mdr.getReplicateOffset().getPlogOffset()
                                    - mdr.getRawRecordSize();
                            }

                            rep.setDataOffset(startOffset);
                            rep.setIdentifier(schemaName);

                            if (!catalog.containsKey(schemaName)) {
                                logger.debug (
                                    "Replication for: "  + schemaName   + " " +
                                    "starting in PLOG: " + plog.getId() + " " +
                                    "at offset: "        + startOffset
                                );

                                catalog.put(schemaName, rep);
                                updateCatalog = true;
                            }
                            else {
                                rep = catalog.get(schemaName);

                                /* update catalog for a sent replicate schema
                                 * when newer replicate offset are found.
                                 * This is needed for when a restart may be 
                                 * triggered, this applies only to schema
                                 * meta data, not LCR data records
                                 */
                                if (rep.sent() &&
                                    rep.getPlogUID() < plog.getUID() &&
                                    rep.getDataOffset() < startOffset)
                                {        
                                    logger.debug (
                                        "Updating existing replication for: " +
                                        schemaName + " starting in PLOG: "    +
                                        plog.getId() + " at offset: "         + 
                                        startOffset
                                    );
                                    rep.setPlogUID(plog.getUID());
                                    rep.setDataOffset(startOffset);
                                }
                            }
                            
                            /* add transaction info using offset of first DDL
                             * only if the connector has been configured to
                             * publish transaction info meta data */
                            if (publishTransactions) {
                                if (!catalog.containsKey(transactionTopicName))
                                {
                                    rep = new ReplicateInfo ();

                                    rep.setPlogUID(plog.getUID());
                                    rep.setDataOffset(startOffset);
                                    rep.setIdentifier(transactionTopicName);
                                    rep.setAggregate(true);
    
                                    catalog.put(transactionTopicName, rep);
                                }
                                else {
                                    rep = catalog.get (transactionTopicName);
                                    
                                    if (rep.sent() &&
                                        rep.getPlogUID() < plog.getUID() &&
                                        rep.getDataOffset() < startOffset)
                                    {
                                        rep.setPlogUID(plog.getUID());
                                        rep.setDataOffset(startOffset);
                                    }
                                }
                            }
                        }
                    }
                }
                
                updateTasks.set(updateCatalog);
                if (updateCatalog) {
                    writeCatalog();
                }
                if (startTasks.get()) {
                    if (updateTasks.get()) {
                        synchronized (updateTasks) {
                            logger.info (
                                "Notify to start tasks at end of PLOG: " +
                                plog.getId()
                            );
                            updateTasks.notify();
                        }
                    }
                }
                else {
                    reconfigureTasks();
                }
            } catch (StreamClosedException se) {
                logger.warn (
                    "Shutting down PLOG monitor, reason: stream closed" 
                );
                
                shutdown();
            }
            catch (InterruptedException ie) {
                /* fine, shutdown in progress */
                logger.warn (ie.getMessage());
            }
            catch (Exception e) {
                logger.warn (
                    "Shutting down PLOG monitor, reason: " + 
                    e.getMessage()
                );
                
                if (logger.isDebugEnabled()) {
                    logger.debug ("Cause: ", e);
                }
                
                shutdown();
                context.raiseError(e);
            }
        
            try {
                boolean shuttingDown = shutdownLatch.await(
                    getWaitTime(), 
                    TimeUnit.MILLISECONDS
                );
                if (shuttingDown) {
                    return;
                }
            } catch (InterruptedException e) {
                /* ignore interrupt */
            }
        }
    }
    
    /**
     * Use the default file manager scan wait time for PLOG monitoring thread's
     * wait time out
     * 
     * @return wait time in milliseconds
     */
    public int getWaitTime() {
        return fileManager.getScanWaitTime();
    }
    
    /**
     * Return file manager's default time out or MAX_TIMEOUT whichever
     * one is shortest
     * 
     * @return timeout in milliseconds
     */
    public int getTimeOut() {
        return MAX_TIMEOUT < fileManager.getTimeOut()
               ? MAX_TIMEOUT 
               : fileManager.getTimeOut();
    }
    
    /**
     * Return configuration of replicate schemas in connector cache for tasks
     * 
     * @return list of replicate schema configuration for tasks, as JSON
     * 
     * @throws Exception if any JSON serialization errors occur
     */
    public synchronized List<String> taskConfigs() throws Exception {
        List <String> configs = new LinkedList <String> ();
        
        if (!active()) {
            throw new InterruptedException();
        }
        
        if (startTasks.get()) {
            synchronized (updateTasks) {
                logger.info ("Waiting for replicated schemas");
                updateTasks.wait();
            }
            startTasks.set(false);
        }
        
        for (ReplicateInfo rep : catalog.values()) {
            /* no need to add sent property to tasks, use default public 
             * view */
            configs.add (rep.toJSONString(true));

            if (!rep.sent()) {
                logger.debug (
                    "Adding replication for: " + rep.toJSONString()
                );
                rep.setSent (true);
                infoTopic.write(rep);
            }
        }
        
        return configs;
    }
    
    /**
     * Request task reconfiguration, if needed
     */
    private void reconfigureTasks () {
        if (updateTasks.get()) {
            logger.info (
                "Request task reconfiguration for updated replication"
            );
            context.requestTaskReconfiguration();
        }
    }
    
    /**
     * Write the catalog entries to the replicate info topic when
     * about to create or update task configuration
     * 
     * @throws Exception for any errors writing to topic
     */
    private void writeCatalog () throws Exception {
        if (updateTasks.get()) {
            /* write current catalog entries to topic */
            for (ReplicateInfo info : catalog.values()) {
                infoTopic.write(info);
            }
        }
    }
    
    /**
     * Check if PLOG monitor is running
     * 
     * @return true if active, else false
     */
    public synchronized boolean active() {
        return isAlive() && shutdownLatch.getCount() > 0;
    }
    
    /** 
     * Shutdown the file manager and latch that controls the monitor
     */
    public synchronized void shutdown() {
        if (active()) {
            logger.info ("Stopping PLOG monitor");
 
            catalog.clear();
            fileManager.close();
            infoTopic.close();
            
            /* set shutdown latch last */
            shutdownLatch.countDown();
            
            if (startTasks.get()) {
                /* if waiting to start task release wait lock */
                synchronized (updateTasks) {
                    updateTasks.notify();
                }
            }
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
            
            logger.info ("Stopped PLOG monitor");
        }
    }
    
    private class ShutdownHook extends Thread {
        private ShutdownHook() {
            setName ("PLOG monitor shutdown");
        }
        @Override
        public void run() {
            try {
                logger.info ("PLOG monitor shutdown requested");
                shutdown();
            } catch (Exception e) { }
        }
    }
}
