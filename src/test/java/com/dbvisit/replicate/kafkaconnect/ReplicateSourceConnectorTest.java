package com.dbvisit.replicate.kafkaconnect;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicateSourceConnectorTest extends ReplicateTestConfig {
    private static final Logger logger = LoggerFactory.getLogger(
        ReplicateSourceConnectorTest.class
    );
    
    final ReplicateSourceConnector connector = new ReplicateSourceConnector() ;

    @Test
    public void testTaskClass() {
      assertEquals(ReplicateSourceTask.class, connector.taskClass());
    }
    
    @Test(expected = ConnectException.class)
    public void testStartupWithInvalidPlogURI() throws Exception {
      HashMap<String, String> props = new HashMap<>();
      
      props.put(
          ReplicateSourceConnectorConfig.PLOG_LOCATION_CONFIG,
          "http://www.google.com"
      );
      connector.start(props);
    }
    
    @Test(expected = ConnectException.class)
    public void testStartupWithInvalidPlogDir() throws Exception {
      HashMap<String, String> props = new HashMap<>();
      
      props.put(
          ReplicateSourceConnectorConfig.PLOG_LOCATION_CONFIG,
          "-"
      );
      connector.start(props);
    }
    
    @Test(expected = ConnectException.class)
    public void testStartupWithNoPlogURI() throws Exception {
        connector.start(new HashMap<String, String>());
    }
    
    @Test
    public void testConfigureSingleTask() throws Exception {
        final int NUM_TASKS = 1;
        /* expect no errors and clean start/stop */
        connector.start(getConfigPropsForMultiSet());
        
        List<Map<String, String>> taskConfs = connector.taskConfigs(NUM_TASKS);
        connector.stop();
        
        Map<String, String> expectedConf = getConfigPropsForSourceTask();
        
        for (Map<String, String> taskConf : taskConfs) {
            for (String configKey : expectedConf.keySet()) {
                String configValue = expectedConf.get(configKey);
                logger.info (
                    "Checking replicate task config: " + configKey + " " +
                    "value: " + configValue
                );
                
                assertTrue (
                    "Expecting replicate task config: " + configKey + " " +
                    "value: " + configValue+ ", got: " +
                    (
                        taskConf.containsKey(configKey) 
                        ? taskConf.get(configKey)
                        : "N/A"
                    ),
                    taskConf.containsKey(configKey) &&
                    taskConf.get(configKey).equals (configValue)
                );
            }
        }
    }
    
    @Test
    public void testConfigureMultipleTasks() throws Exception {
        final int NUM_TASKS = 2;
        /* expect no errors and clean start/stop */
        connector.start(getConfigPropsForMultiSet());
        
        List<Map<String, String>> taskConfs = connector.taskConfigs(NUM_TASKS);
        connector.stop();
        
        List<Map<String, String>> expectedConfs =
            new ArrayList<Map<String, String>>();
        
        /* same config test fixture with replicate schemas split */
        final Map<String, String> commonConf = getConfigPropsForMultiSet();
        
        final String []repJSONs = getConfigPropsForSourceTask().get (
            ReplicateSourceTaskConfig.REPLICATED_CONFIG
        ).split (
            Pattern.quote(
                ReplicateSourceConnector.REPLICATED_GROUP_DELIMTER
            )
        );
        
        assertTrue (repJSONs.length == NUM_TASKS);
        
        for (int i = 0; i < NUM_TASKS; i++) {
            Map<String, String> confProps =
                new HashMap<String, String>(commonConf);
            
            confProps.put (
                ReplicateSourceTaskConfig.REPLICATED_CONFIG,
                repJSONs[i]
            );
            expectedConfs.add (confProps);
        }
        
        assertTrue (
            "Expecting configuration for: " + NUM_TASKS + " tasks, got: " +
             taskConfs.size(),
             taskConfs.size() == NUM_TASKS
        );
        
        for (int i = 0; i < NUM_TASKS; i++) {
            Map<String, String> taskConf = taskConfs.get(i);
            Map<String, String> expectedConf = expectedConfs.get(i);
            
            for (String configKey : expectedConf.keySet()) {
                String configValue = expectedConf.get(configKey);
                logger.info (
                    "Checking replicate task config: " + configKey + " " +
                    "value: " + configValue
                );
                
                assertTrue (
                    "Expecting replicate task config: " + configKey + " " +
                    "value: " + configValue + ", got: " +
                    (
                        taskConf.containsKey(configKey) 
                        ? taskConf.get(configKey)
                        : "N/A"
                    ),
                    taskConf.containsKey(configKey) &&
                    taskConf.get(configKey).equals (configValue)
                );
            }
        }
    }
    
}
