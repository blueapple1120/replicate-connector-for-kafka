package com.dbvisit.replicate.kafkaconnect;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.easymock.EasyMock;
import org.easymock.TestSubject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.kafkaconnect.util.ReplicateInfoTopic;

@RunWith(PowerMockRunner.class)
public class ReplicateSourceConnectorTest extends ReplicateTestConfig {
    private static final Logger logger = LoggerFactory.getLogger(
        ReplicateSourceConnectorTest.class
    );

    @Mock ConnectorContext context;
    @TestSubject
    final ReplicateSourceConnector connector = new ReplicateSourceConnector();

    @Test
    public void testTaskClass() {
        assertEquals(ReplicateSourceTask.class, connector.taskClass());
    }
    
    @Test(expected = ConnectException.class)
    public void testStartupWithInvalidPlogURI() throws Exception {
        connector.initialize(context);
        PowerMock.mockStatic (ReplicateInfoTopic.class);
        EasyMock.expect (
            ReplicateInfoTopic.composer()
        ).andReturn(topicConfigurator);
        PowerMock.replayAll();
        HashMap<String, String> props = new HashMap<>();
      
        props.put(
            ReplicateSourceConnectorConfig.PLOG_LOCATION_CONFIG,
            "http://www.google.com"
        );
        connector.start(props);
    }
    
    @Test(expected = ConnectException.class)
    public void testStartupWithInvalidPlogDir() throws Exception {
        connector.initialize(context);
        PowerMock.mockStatic (ReplicateInfoTopic.class);
        EasyMock.expect (
            ReplicateInfoTopic.composer()
        ).andReturn(topicConfigurator);
        PowerMock.replayAll();
        
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
        connector.initialize(context);
        PowerMock.mockStatic (ReplicateInfoTopic.class);
        EasyMock.expect (
            ReplicateInfoTopic.composer()
        ).andReturn(topicConfigurator);
        PowerMock.replayAll();
        
        /* expect no errors and clean start/stop */
        connector.start(getConfigPropsForMultiSet());
        
        List<Map<String, String>> taskConfs = connector.taskConfigs(NUM_TASKS);
        connector.stop();
        
        logger.info (taskConfs.toString());
        
        Map<String, String> expectedConf = getConfigPropsForSourceConnector();
        
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
    
    // component integration test
    @Test
    public void testConfigureMultipleTasks() throws Exception {
        final int NUM_TASKS = 2;
        connector.initialize(context);
        PowerMock.mockStatic (ReplicateInfoTopic.class);
        EasyMock.expect (
            ReplicateInfoTopic.composer()
        ).andReturn(topicConfigurator);
        PowerMock.replayAll();

        try {
            /* expect no errors and clean start/stop */
            connector.start(getConfigPropsForMultiSet());
            PowerMock.verifyAll();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        
        List<Map<String, String>> taskConfs = connector.taskConfigs(NUM_TASKS);

        connector.stop();
        
        List<Map<String, String>> expectedConfs =
            new ArrayList<Map<String, String>>();
        
        /* same config test fixture with replicate schemas split */
        final Map<String, String> commonConf = getConfigPropsForMultiSet();
        
        final String []repJSONs = getConfigPropsForSourceConnector().get (
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
