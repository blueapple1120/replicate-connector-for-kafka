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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dbvisit.replicate.kafkaconnect.util.Version;
import com.dbvisit.replicate.kafkaconnect.util.StringUtils;

/**
 * Replicate Kafka Source Connector
 */
public class ReplicateSourceConnector extends SourceConnector {
    /** Delimiter to use to separate replicated groups encoded in JSON */
    public static final String REPLICATED_GROUP_DELIMTER = "|";
    /** Configuration properties */
    private Map <String, String> configProps;
    /** PLOG monitor */
    private PlogMonitorThread plogMonitorThread;

    private static final Logger logger = LoggerFactory.getLogger(
        ReplicateSourceConnector.class
    );
    
    /**
     * Return version of the replicate source connector
     * 
     * @return version of connector, formatted as string
     */
    @Override
    public String version () {
        return Version.getVersion();
    }

    /**
     * Start the replicate source connector and PLOG monitoring.
     * 
     * @param props connector configuration properties
     * @throws ConnectException for any start up errors 
     */
    @Override
    public void start (Map<String, String> props) throws ConnectException {
        try {
            configProps = props;
            plogMonitorThread = new PlogMonitorThread(
                context, 
                configProps
            );
        } catch (Exception e) {
            throw new ConnectException(
                "Couldn't start ReplicateSourceConnector due to " + 
                "configuration error", e
            );
        }
        
        logger.debug ("Starting PLOG monitoring thread");
        
        /* start monitoring prepared tables in PLOGs */
        plogMonitorThread.start();
        
        try {
            synchronized (plogMonitorThread) {
                /* block until first batch of replicated schemas are ready */
                logger.info ("Waiting for replication to start");
                plogMonitorThread.wait();
                
                if (!plogMonitorThread.isAlive()) {
                    throw new Exception ("PLOG monitoring thread is done");
                }
                
                /* tasks will now be started and ready to start monitoring
                 * updates to schema registry */
                plogMonitorThread.startupDone();
            }
            
            logger.info ("PLOG monitoring thread started");
        } catch (Exception e) {
            throw new ConnectException (
                "Failed to wait for replicated schemas: " + e.getMessage()
            );
        }
    }

    /**
     * Returns the task implementation for this connector
     * 
     * @return Class definition for source task to start
     */
    @Override
    public Class<? extends Task> taskClass () {
        return ReplicateSourceTask.class;
    }

    /**
     * Return the configuration for starting a replicate source task, 
     * which includes the information of the replicate schema that
     * the tasks needs to process
     * 
     * @param maxTasks the maximum number of tasks to configure
     * 
     * @return List of property map
     */
    @Override
    public List<Map<String, String>> taskConfigs (int maxTasks) {
        List<Map<String, String>> taskConfigs = null;
        
        try {
            List<String> replicated = plogMonitorThread.replicatedSchemas();

            int numGroups = Math.min(replicated.size(), maxTasks);
            
            List<List<String>> groups = 
                ConnectorUtils.groupPartitions(replicated, numGroups);
            
            taskConfigs = new ArrayList<>(groups.size());

            for (List<String> group : groups) {
                logger.debug ("Adding replicate config : " + group);
                
                Map<String, String> taskProps = new HashMap<>(configProps);
                taskProps.put (
                    ReplicateSourceTaskConfig.REPLICATED_CONFIG,
                    StringUtils.join (group, REPLICATED_GROUP_DELIMTER)
                );
                taskConfigs.add(taskProps);
            }
        }
        catch (Exception e) {
            logger.error (e.getMessage());
        }
        
        return taskConfigs;
    }

    /**
     * Stop the replicate source connector which includes shutting down
     * the monitoring of PLOGs
     * 
     *  @throws ConnectException when shutting down failed
     */
    @Override
    public void stop () throws ConnectException {
        logger.debug ("Stopping PLOG monitoring thread");
        plogMonitorThread.shutdown();
        
        try {
            plogMonitorThread.join(plogMonitorThread.getTimeOut());
        } catch (InterruptedException e) {
            /* ignore */
        }
    }

    /** 
     * Return replicate source connector configuration
     * 
     * @return Configuration definition
     */
    public ConfigDef config () {
        return ReplicateSourceConnectorConfig.config;
    }

}
