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
    private PlogMonitorThread monitorThread;

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
            
            logger.debug ("Creating PLOG monitor");
            monitorThread = PlogMonitorThread.composer()
                .configure(configProps)
                .context(context) 
                .build();
        } catch (Exception e) {
            throw new ConnectException(
                "Couldn't start ReplicateSourceConnector due to " + 
                "configuration error", e
            );
        }
        
        /* start monitoring meta data for replicated tables in PLOGs */
        logger.debug ("Starting PLOG monitor");
        monitorThread.start();
        
        if (!monitorThread.active()) {
            throw new ConnectException (
                "PLOG monitor is not running, cannot continue"
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
            List<String> configs = monitorThread.taskConfigs();
            
            logger.debug (
                "Source connector received task configuration: " +
                configs.toString() + " from PLOG monitor"
            );
            
            int numGroups = Math.min(configs.size(), maxTasks);
            
            List<List<String>> groups = 
                ConnectorUtils.groupPartitions(configs, numGroups);
            
            taskConfigs = new ArrayList<>(groups.size());

            for (List<String> group : groups) {
                logger.debug ("Adding replicate task config : " + group);
                
                Map<String, String> taskProps = new HashMap<>(configProps);
                taskProps.put (
                    ReplicateSourceTaskConfig.REPLICATED_CONFIG,
                    StringUtils.join (group, REPLICATED_GROUP_DELIMTER)
                );
                taskConfigs.add(taskProps);
            }
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                /* interrupted for shutdown, return empty list */
                taskConfigs = new ArrayList<>();
            }
            else {
                logger.error (e.getMessage());
                context.raiseError(e);
            }
        }
        
        return taskConfigs;
    }

    /**
     * Stop the replicate source connector which includes shutting down
     * the monitoring of PLOGs
     * 
     * @throws ConnectException when shutting down failed
     */
    @Override
    public void stop () throws ConnectException {
        monitorThread.shutdown();
        
        try {
            monitorThread.join(monitorThread.getTimeOut());
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
