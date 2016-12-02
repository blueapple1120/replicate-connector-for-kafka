package com.dbvisit.replicate.kafkaconnect;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Test;
import org.powermock.api.easymock.annotation.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.ReplicateInfo;

public class PlogMonitoringThreadTest extends ReplicateTestConfig {
    private static final Logger logger = LoggerFactory.getLogger (
        PlogMonitoringThreadTest.class
    );
    
    @Mock private ConnectorContext context;
    private PlogMonitorThread plogMonitorThread;

    @Test
    public void testMonitorReplicatedSchema() {
        /* test PLOG data contains one replicated table and it's
         * transaction meta data 
           {
               "plogId":20,
               "dataOffset":12812,
               "identifier":"TXMETA",
               "aggregate":true
           }, 
           {
              "plogId":20,
               "dataOffset":12812,
               "identifier":"SOE.UNITTEST",
               "aggregate":false
           }
         */
      
        @SuppressWarnings("serial")
        final HashMap<String, ReplicateInfo> EXPECTED_REP_INFO = 
            new HashMap<String, ReplicateInfo>() {{
                put (
                    EXPECTED_AGGREGATE,
                    EXPECTED_AGGREGATE_INFO
                );
                put (
                    EXPECTED_TABLE,
                    EXPECTED_TABLE_INFO
                );
        }};
        
        List<String> replicatedAsJSON = null;
        
        try {
            plogMonitorThread = new PlogMonitorThread(
                context, 
                getConfigPropsForMultiSet()
            );
        } catch (Exception e) {
            fail (
                "Couldn't start ReplicateSourceConnector due to " + 
                "configuration error"
            );
        }
        
        logger.info ("Starting PLOG monitoring thread");
        
        /* start monitoring prepared tables in PLOGs */
        plogMonitorThread.start();
        
        try {
            synchronized (plogMonitorThread) {
                while (plogMonitorThread.isAlive() &&
                       !plogMonitorThread.readyForTasks())
                {
                    logger.info (
                        "Waiting for replicated schema or until timeout"
                    );

                    plogMonitorThread.wait(plogMonitorThread.getTimeOut());
                }
            }

            assertTrue (
                "Must have replicated schemas",
                plogMonitorThread.readyForTasks()
            );
            
            try {
                replicatedAsJSON = plogMonitorThread.replicatedSchemas();
            } catch (Exception e1) {
                e1.printStackTrace();
                fail (e1.getMessage());
            }
        } catch (Exception e) {
            fail (
                "Failed to wait for replicated schemas: " + e.getMessage()
            );
        }
        
        assertTrue (
            "Expecting: " + EXPECTED_REP_SIZE + " replicated info, got: " +
            (replicatedAsJSON != null ? replicatedAsJSON.size() : "None"),
            replicatedAsJSON != null &&
            replicatedAsJSON.size() == EXPECTED_REP_SIZE
        );
        
        Map <String, ReplicateInfo> replicated = 
            new HashMap <String, ReplicateInfo>();
        
        for (String repJSON : replicatedAsJSON) {
            logger.info ("Replicated Info: " + repJSON);
            ReplicateInfo rep = null;
            try {
                rep = ReplicateInfo.fromJSONString(repJSON);
            } catch (Exception e) {
                e.printStackTrace();
                fail (e.getMessage());
            }
            
            replicated.put (rep.getIdentifier(), rep);
        }
        
        for (String repId : EXPECTED_REP_INFO.keySet()) {
            assertTrue (
                "Expecting replicated info for: " + repId,
                replicated.containsKey(repId)
            );
            ReplicateInfo EXPECTED = EXPECTED_REP_INFO.get (repId);
            ReplicateInfo repInfo = replicated.get (repId);
            assertTrue (
                "Expecting replicate info: " + EXPECTED.toString() + 
                ", got: " + repInfo.toString(),
                repInfo.getIdentifier().equals(EXPECTED.getIdentifier()) &&
                repInfo.getPlogUID().equals(EXPECTED.getPlogUID()) &&
                repInfo.getDataOffset().equals(EXPECTED.getDataOffset()) &&
                repInfo.isAggregate() == EXPECTED.isAggregate()
            );
        }
        
        plogMonitorThread.shutdown();
        
        try {
            plogMonitorThread.join(plogMonitorThread.getTimeOut());
        } catch (InterruptedException e) {
            /* ignore */
        }
        
        assertFalse (
            "Monitoring thread is done",
            plogMonitorThread.isAlive()
        );
    }

}
