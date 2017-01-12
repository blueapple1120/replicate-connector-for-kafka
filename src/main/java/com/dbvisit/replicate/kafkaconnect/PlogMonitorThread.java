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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.file.PlogFile;
import com.dbvisit.replicate.plog.file.PlogFileManager;
import com.dbvisit.replicate.plog.format.EntrySubType;
import com.dbvisit.replicate.plog.format.EntryType;
import com.dbvisit.replicate.plog.format.parser.Parser.StreamClosedException;
import com.dbvisit.replicate.plog.metadata.DDLMetaData;
import com.dbvisit.replicate.plog.reader.DomainReader;
import com.dbvisit.replicate.plog.reader.PlogStreamReader;
import com.dbvisit.replicate.plog.reader.criteria.Criteria;
import com.dbvisit.replicate.plog.reader.criteria.InternalDDLFilterCriteria;
import com.dbvisit.replicate.plog.reader.criteria.TypeCriteria;
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
public class PlogMonitorThread extends Thread {
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
    /** all replicated schemas monitored */
    private Map <String, ReplicateInfo> replicated;
    /** entry record read criteria for JSON meta data */
    private Criteria<EntrySubType> parseCriteria;
    /** Plog record sub type persistence criteria for JSON DDL */
    private TypeCriteria<EntrySubType> persistCriteria;
    /** Internal DDL filter */
    private InternalDDLFilterCriteria<DomainRecordType> ddlFilter;
    /** Whether or not it's starting monitoring of PLOGs for schema */
    private boolean startup;
    /** Number of replicated schema information sent to connector */
    private int numRepSent = 0;
    /** Internal transaction info topic */
    private String transactionInfoName;
    /** Delay processing until first PLOG batch has been processed */
    private boolean firstRun;

    /**
     * Create PLOG monitoring thread 
     * 
     * @param context Connector context to use for monitoring thread to
     *                request task reconfiguration
     * @param props   Connector and PLOG reader configuration properties
     * 
     * @throws Exception when any startup error occurs
     */
    public PlogMonitorThread (
        ConnectorContext context, 
        Map<String, String> props
    ) throws Exception 
    {
        if (props == null) {
            throw new Exception (
                "No configuration properties provided, cannot start " +
                "monitoring PLOG location"
            );
        }
        fileManager   = new PlogFileManager (new PlogConfig(props));
        this.context  = context;
        shutdownLatch = new CountDownLatch(1);
        replicated    = new ConcurrentHashMap<String, ReplicateInfo> ();
        startup       = true;
        firstRun      = true;
        
        parseCriteria = 
            new TypeCriteria<EntrySubType> (subtypes);
        persistCriteria = 
            new TypeCriteria<EntrySubType> (persistent);
        ddlFilter = 
            new InternalDDLFilterCriteria<DomainRecordType> ();
        
        String prop = 
            ReplicateSourceConnectorConfig.TOPIC_NAME_TRANSACTION_INFO_CONFIG;
        String defval =
            ReplicateSourceConnectorConfig.TOPIC_NAME_TRANSACTION_INFO_DEFAULT;
                
        if (props.containsKey (prop)) {
            transactionInfoName = props.get (prop);
        }
        else {
            transactionInfoName = defval;
        }
    }
    
    /**
     * Monitor replicated schemas in PLOG stream 
     */
    @Override
    public void run() {
        logger.info ("Running PLOG monitoring thread");
        
        PlogFile plog = null;
        ReplicateInfo rep = null;
        
        fileManager.setForceInterrupt();
        
        while (shutdownLatch.getCount() > 0) {
            try {
                /* the source connector context do not have access to the
                 * offset reader to determine the last processed PLOG.
                 * Therefore we always start at oldest PLOG and notify
                 * tasks as if it was a cold start. The tasks has access 
                 * to the offset reader and will update the replicated
                 * schema information accordingly
                 */
                fileManager.scan();
                plog = fileManager.getPlog();
                
                logger.info ("Monitoring PLOG: " + plog.getFileName());
            }
            catch (Exception e) {
                if (!(e instanceof InterruptedException)) {
                    logger.error (
                        "An unrecoverable error has occurred, error: " +
                        e.getMessage() + ", shutting down PLOG monitor"
                    );
                    
                    if (logger.isDebugEnabled()) {
                        logger.debug ("Cause: ", e);
                    }
                }
                shutdown();
                continue;
            }
            
            /* limit reading to JSON DDL records, these are emitted when 
             * new table is added to replication
             */
            PlogStreamReader reader = plog.getReader();
            
            DomainReader domainReader = reader.getDomainReader();
            
            /* set filter criteria for meta data */
            domainReader.setParseCriteria(parseCriteria);
            /* set persist criteria */
            domainReader.setPersistCriteria(persistCriteria);
            /* set parser */
            domainReader.setDomainParsers(domainParsers);
            
            /* flush after record, could change in future */
            reader.setFlushSize(1);
            
            try {
                int newRep = 0;
                
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
                        if (ddlFilter.meetCriteria(mdr) &&
                            !mdr.getMetaData().fromRecycleBin())
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

                            if (!replicated.containsKey(schemaName)) {
                                logger.info (
                                    "Replication for: "  + schemaName   + " " +
                                    "starting in PLOG: " + plog.getId() + " " +
                                    "at offset: "        + startOffset
                                );

                                replicated.put(schemaName, rep);
                                newRep++;
                            }
                            else {
                                rep = replicated.get(schemaName);

                                /* update it's PLOG and offset for when a 
                                 * restart may be triggered if it has 
                                 * already been sent to task, this applies
                                 * for meta data only, not LCR data records
                                 */
                                if (rep.sent()) {
                                    rep.setPlogUID(plog.getUID());
                                    rep.setDataOffset(startOffset);
                                }
                            }
                            
                            /* add transaction info using offset of first DDL */
                            if (!replicated.containsKey(transactionInfoName)) {
                                rep = new ReplicateInfo ();

                                rep.setPlogUID(plog.getUID());
                                rep.setDataOffset(startOffset);
                                rep.setIdentifier(transactionInfoName);
                                rep.setAggregate(true);

                                replicated.put(transactionInfoName, rep);
                                newRep++;
                            }
                            else {
                                /* update it */
                                rep = replicated.get (transactionInfoName);
                                
                                if (rep.sent()) {
                                    rep.setPlogUID(plog.getUID());
                                    rep.setDataOffset(startOffset);
                                }
                            }
                        }
                    }
                }
                
                /* first run done, only send replicate schemas now, if
                 * we have any 
                 */
                if (firstRun) {
                    firstRun = false;
                }
                
                /* monitor is either in startup or update mode */
                if (startup) {
                    /* when in start up mode notify at end of PLOG so that 
                     * source connector can start tasks to handle 
                     * replication 
                     */ 
                    synchronized (this) {
                        logger.debug (
                            "Checking if replication is ready for tasks at " +
                            "the end of PLOG: " + plog.getFileName() + " "   +
                            "ready: " + readyForTasks()
                        );
                        
                        if (readyForTasks()) {
                            this.notifyAll();
                        }
                    }
                }
                else {
                    /* request source connector to re-configure kafka tasks 
                     * when updated or new replicated tables arrive, only
                     * when kafka tasks have already started up and
                     * previous batches have already been sent, only done at 
                     * end of PLOG
                     */
                    if (numRepSent > 0 && 
                        newRep     > 0 &&
                        numRepSent < replicated.size())
                    {
                        /* trigger a reconfiguration if a previous batch 
                         * has already been sent, this should not be done 
                         * too often else tasks get's constantly 
                         * reconfigured therefore we do it at end of PLOG
                         */
                        logger.info (
                            "Request task reconfiguration for " + newRep + " " +
                            "replicated tables added in PLOG: " + plog.getId()
                        );
                        newRep = 0;
                        context.requestTaskReconfiguration();
                    }
                }
            } catch (StreamClosedException se) {
                logger.warn (
                    "Shutting down PLOG monitor, reason: stream closed" 
                );
                
                shutdown();
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
     * Returns new entries in the cache of replicate schemas monitor
     * 
     * @return list of replicate schema serialized as JSON
     * 
     * @throws Exception if any JSON serialization errors occur
     */
    public List<String> replicatedSchemas() throws Exception {
        List <String> toAdd = new LinkedList <String> ();
        
        for (String repName : replicated.keySet()) {
            ReplicateInfo rep = replicated.get (repName);
            
            logger.debug (
                "Adding replication for: " + rep.toJSONString()
            );

            /* has not been added */
            toAdd.add (rep.toJSONString());
            
            if (!rep.sent()) {
                rep.setSent (true);
                numRepSent++;
            }
        }
        
        return toAdd;
    }
    
    /**
     * Check whether or not the monitor has gathered enough details about
     * replication for kafka source tasks to tasks processing replicated
     * data. This is delayed to after first run is complete to prevent 
     * frequent restarts in first PLOG
     * 
     * @return true of false, whether or not replicated schemas have been 
     *         seen by monitor
     */
    public boolean readyForTasks () {
        /* not the first run and has replicated schema data to process */
        return (!firstRun && replicated.size() > 0);
    }
    
    /** 
     * Shutdown the file manager and latch that controls the monitor
     */
    public void shutdown() {
        fileManager.close();
        shutdownLatch.countDown();
    }
    
    /**
     * Set internal state to indicate that the first initial startup has
     * been done, the monitor then transitions into a update mode
     */
    public void startupDone () {
        startup = false;
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
    
    /** Domain parses include DDL JSON parser and proxy parser for
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
