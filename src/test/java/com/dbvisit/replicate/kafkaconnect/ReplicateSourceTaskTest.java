package com.dbvisit.replicate.kafkaconnect;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.ReplicateOffset;

/** Simple tests to validate starting on replicate source task */
public class ReplicateSourceTaskTest extends ReplicateTestConfig {
    private static final Logger logger = LoggerFactory.getLogger (
        ReplicateSourceTaskTest.class
    );
        
    /** Validate record count per PLOG */
    private void validateRecordCount (int recordCount) {
        assertEquals (
            "Expecting " + EXPECTED_NUM_MESSAGES_PER_PLOG + " " + 
            "records per PLOG poll",
            EXPECTED_NUM_MESSAGES_PER_PLOG,
            recordCount
        );
    }
    
    /** Validate record topics per PLOG */
    private void validateTopics (List<SourceRecord> records) {
        for (int i = 0; i < EXPECTED_NUM_MESSAGES_PER_PLOG; i++) {
            String topic = EXPECTED_TOPICS[i];
            SourceRecord record = records.get (i);
            assertTrue (
                "Expecting topic: " + topic + ", got: " + record.topic(),
                record.topic().equals (topic)
            );
        }
    }
    
    /** Validate record offset */
    private void validateRecordOffset (
        List<SourceRecord> records, ReplicateOffset storedOffset
    ) throws Exception 
    {
        for (SourceRecord record : records) {
            ReplicateOffset recordOffset = ReplicateOffset.fromJSONString(
                (String)
                record.sourceOffset().get(
                    ReplicateSourceTask.REPLICATE_OFFSET_KEY
                )
            );
            assertTrue (
                "Expecting records that are newer than stored offset: " +
                storedOffset,
                storedOffset.compareTo(recordOffset) < 0
            );
        }
    }
    
    @Test
    public void testSourceTaskProcessAll() {
        logger.info (
            "Setting up stubs for clean run with no published messages"
        );
        
        SourceTaskContext context = EasyMock.createMock(
            SourceTaskContext.class
        );
        
        OffsetStorageReader offsetStorageReader = EasyMock.createMock (
            OffsetStorageReader.class
        );
        
        EasyMock.expect(context.offsetStorageReader())
            .andReturn(offsetStorageReader);
        
        EasyMock.expect(offsetStorageReader.offsets(offsetPartitions()))
            .andReturn(new HashMap<Map<String, String>, Map<String, Object>>());
        
        EasyMock.checkOrder(context, false);
        EasyMock.replay(context);
        
        EasyMock.checkOrder(offsetStorageReader, false);
        EasyMock.replay(offsetStorageReader);
        
        logger.info ("Source task poll all - test");
        
        ReplicateSourceTask sourceTask = new ReplicateSourceTask();
        
        sourceTask.initialize(context);
        
        sourceTask.start(getConfigPropsForSourceTask());
        
        try {
            assertEquals (
                "Expecting no record for first poll, nothing in PLOG", 
                null, 
                sourceTask.poll()
            );
            
            /* expect 2 messages per PLOG, 3 PLOGs with data */
            List<SourceRecord> records = sourceTask.poll();
            validateRecordCount (records.size());
            validateTopics (records);
            
            records = sourceTask.poll();
            validateRecordCount (records.size());
            validateTopics (records);
            
            records = sourceTask.poll();
            validateRecordCount (records.size());
            validateTopics (records);
        }
        catch (InterruptedException ie) {
            logger.info ("Stopping task");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            sourceTask.stop();
        }
    }
    
    @Test
    public void testSourceTaskProcessNoneByStoredOffset() {
        logger.info ("Setting up stubs using fake committed offsets");
        
        SourceTaskContext context = EasyMock.createMock(
            SourceTaskContext.class
        );
        
        OffsetStorageReader offsetStorageReader = EasyMock.createMock (
            OffsetStorageReader.class
        );
        
        EasyMock.expect(context.offsetStorageReader())
            .andReturn(offsetStorageReader);
        
        try {
            EasyMock.expect(offsetStorageReader.offsets(offsetPartitions()))
                .andReturn(
                    storedOffsets (PLOG_23_TRANSACTION_OFFSET.toJSONString())
                );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        
        EasyMock.checkOrder(context, false);
        EasyMock.replay(context);
        
        EasyMock.checkOrder(offsetStorageReader, false);
        EasyMock.replay(offsetStorageReader);
        
        logger.info ("Source task poll all committed, no new data");
        
        ReplicateSourceTask sourceTask = new ReplicateSourceTask();
        
        sourceTask.initialize(context);
        
        try {
            sourceTask.start(getConfigPropsForSourceTask());
        }
        catch (ConnectException ce) {
            ce.printStackTrace();
        }
        
        try {
            /* expect no new messages to publish */
            List<SourceRecord> records = sourceTask.poll();
            assertNull (
                "Expecting no records when all records have been published",
                records
            );
        }
        catch (InterruptedException ie) {
            logger.info ("Stopping task");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            sourceTask.stop();
        }
    }
    
    @Test
    public void testSourceTaskProcessLastMessagesByStoredOffset() {
        logger.info ("Setting up stubs using fake committed offsets");
        
        SourceTaskContext context = EasyMock.createMock(
            SourceTaskContext.class
        );
        
        OffsetStorageReader offsetStorageReader = EasyMock.createMock (
            OffsetStorageReader.class
        );
        
        EasyMock.expect(context.offsetStorageReader())
            .andReturn(offsetStorageReader);
        
        try {
            EasyMock.expect(offsetStorageReader.offsets(offsetPartitions()))
                .andReturn(
                    storedOffsets (PLOG_22_TRANSACTION_OFFSET.toJSONString())
                );
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        
        EasyMock.checkOrder(context, false);
        EasyMock.replay(context);
        
        EasyMock.checkOrder(offsetStorageReader, false);
        EasyMock.replay(offsetStorageReader);
        
        logger.info ("Source task poll all committed, no new data");
        
        ReplicateSourceTask sourceTask = new ReplicateSourceTask();
        
        sourceTask.initialize(context);
        
        sourceTask.start(getConfigPropsForSourceTask());
        
        try {
            /* expect no new messages to publish for first poll */
            List<SourceRecord> records = sourceTask.poll();
            assertNull (
                "Expecting no records for PLOGs up to 22 as filtered " +
                "offsets parse criteria",
                records
            );
            
            /* expect 2 messages for last PLOG */
            records = sourceTask.poll();
            validateRecordCount (records.size());
            validateTopics (records);
            validateRecordOffset(records, PLOG_22_TRANSACTION_OFFSET);
            
            /* next call will block because there is no further data */
        }
        catch (InterruptedException ie) {
            logger.info ("Stopping task");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            sourceTask.stop();
        }
    }
    
    @Test
    public void testSourceTaskProcessTwoMessagesBySCN() {
        logger.info (
            "Setting up stubs using global SCN filter for fake cold start"
        );
        
        SourceTaskContext context = EasyMock.createMock(
            SourceTaskContext.class
        );
        
        OffsetStorageReader offsetStorageReader = EasyMock.createMock (
            OffsetStorageReader.class
        );
        
        EasyMock.expect(context.offsetStorageReader())
            .andReturn(offsetStorageReader);
        
        EasyMock.expect(offsetStorageReader.offsets(offsetPartitions()))
            .andReturn(new HashMap<Map<String, String>, Map<String, Object>>());
        
        EasyMock.checkOrder(context, false);
        EasyMock.replay(context);
        
        EasyMock.checkOrder(offsetStorageReader, false);
        EasyMock.replay(offsetStorageReader);
        
        logger.info ("Source task poll all committed, no new data");
        
        ReplicateSourceTask sourceTask = new ReplicateSourceTask();
        
        sourceTask.initialize(context);
        
        sourceTask.start(
            getConfigPropsForSourceTaskWithStartSCN(PLOG_22_TRANSACTION_SCN)
        );
        
        try {
            /* expect no new messages to publish for first poll */
            List<SourceRecord> records = sourceTask.poll();
            assertNull (
                "Expecting no records for first PLOG 20, contains no data",
                records
            );
            
            records = sourceTask.poll();
            assertNull (
                "Expecting no records for second PLOG",
                records
            );
            
            /* expect 2 messages for last two PLOG */
            records = sourceTask.poll();
            validateRecordCount (records.size());
            validateTopics (records);
            validateRecordOffset(records, PLOG_21_TRANSACTION_OFFSET);
            
            records = sourceTask.poll();
            validateRecordCount (records.size());
            validateTopics (records);
            validateRecordOffset(records, PLOG_22_TRANSACTION_OFFSET);
            
            /* next call will block because there is no further data */
        }
        catch (InterruptedException ie) {
            logger.info ("Stopping task");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
        finally {
            sourceTask.stop();
        }
    }

}
