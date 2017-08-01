package com.dbvisit.replicate.kafkaconnect;

import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.plog.domain.ColumnValue;
import com.dbvisit.replicate.plog.domain.DomainRecord;
import com.dbvisit.replicate.plog.domain.ChangeRowRecord;
import com.dbvisit.replicate.plog.domain.ChangeSetRecord;
import com.dbvisit.replicate.plog.domain.ReplicateOffset;
import com.dbvisit.replicate.plog.domain.TransactionInfoRecord;

/**
 * Test the record conversion in isolation, not using source task and using
 * mock schema definition 
 */
public class ReplicateRecordConversionTest extends ReplicateTestConfig {
    private static final Logger logger = LoggerFactory.getLogger (
        ReplicateRecordConversionTest.class
    );
    final private ConnectorMode rowTxMetaMode = new ConnectorMode (
        ConnectorCDCFormat.CHANGEROW,
        true,
        false,
        false
    );
    final private ConnectorMode setTxMetaMode = new ConnectorMode (
        ConnectorCDCFormat.CHANGESET,
        true,
        false,
        false
    );
    final private ConnectorMode rowNoTxMetaMode = new ConnectorMode (
        ConnectorCDCFormat.CHANGEROW,
        false,
        false,
        false
    );
    final private ConnectorMode rowNoTxMetaWithKeysMode = new ConnectorMode (
        ConnectorCDCFormat.CHANGEROW,
        false,
        true,
        false
    );

    @SuppressWarnings("serial")
    @Test
    public void testConvertChangeRowRecords() {
        try {
            final LinkedHashMap<String, Schema> fieldSchemas =
                new LinkedHashMap<String, Schema>() {{
                    put ("XID", Schema.STRING_SCHEMA);
                    put ("TYPE", Schema.STRING_SCHEMA);
                    put ("CHANGE_ID", Schema.INT64_SCHEMA);
                    put ("ID", Schema.INT32_SCHEMA);
                    put ("TEST_NAME", Schema.STRING_SCHEMA);
                }};
                
            final LinkedList<Field> fields = 
                new LinkedList<Field>() {{
                    add(
                        new Field(
                            "XID", 
                            0, 
                            fieldSchemas.get("XID")
                        )
                    );
                    add(
                        new Field(
                            "TYPE", 
                            1, 
                            fieldSchemas.get("TYPE")
                        )
                    );
                    add(
                        new Field(
                            "CHANGE_ID",
                            2,
                            fieldSchemas.get("CHANGE_ID")
                        )
                    );
                    add(
                        new Field(
                            "ID",
                            3,
                            fieldSchemas.get("ID")
                        )
                    );
                    add(
                        new Field(
                            "TEST_NAME",
                            4,
                            fieldSchemas.get("TEST_NAME")
                        )
                    );
                }};
            
            final ReplicateOffset [] expectedOffsets = new ReplicateOffset[] {
                PLOG_21_CHANGE_OFFSET,
                PLOG_22_CHANGE_OFFSET,
                PLOG_23_CHANGE_OFFSET
            };
                    
            final String mockName = "MOCK-SOE.UNITTEST"; 
                
            for (int i = 0; i < CHANGEROW_RECORDS_JSON.length; i++) {
                Schema mockSchema = EasyMock.createMock(Schema.class);
                TopicSchema schema = new TopicSchema(null,  mockSchema);
                
                EasyMock.expect (mockSchema.name())
                    .andReturn(mockName);
                EasyMock.expect (mockSchema.type())
                    .andReturn(Schema.Type.STRUCT);
                EasyMock.expect (mockSchema.fields())
                    .andReturn(fields);
                EasyMock.expect (mockSchema.field("XID"))
                    .andReturn(fields.get(0));
                EasyMock.expect (mockSchema.field("TYPE"))
                    .andReturn(fields.get(1));
                EasyMock.expect (mockSchema.field("CHANGE_ID"))
                    .andReturn(fields.get(2));
                EasyMock.expect (mockSchema.field("ID"))
                    .andReturn(fields.get(3));
                EasyMock.expect (mockSchema.field("TEST_NAME"))
                    .andReturn(fields.get(4));
                EasyMock.replay (mockSchema);
                
                ChangeRowRecord inrec = 
                    ChangeRowRecord.fromJSONString(CHANGEROW_RECORDS_JSON[i]);
                
                /* we do not serialize/de-serialize replicate offset, it's
                 * redundant and transient
                 */
                inrec.setReplicateOffset(expectedOffsets[i]);
            
                logger.info ("In: " + inrec.toJSONString());
                     
                SourceRecord outrec = 
                    new ReplicateRecordConverter()
                        .schema (schema)
                        .record ((DomainRecord)inrec)
                        .mode(rowTxMetaMode)
                        .convert();
                    
                Struct kstruct = (Struct)outrec.value();
                     
                logger.info (
                    "Out: " +
                    "topic=" + outrec.topic()                      + " " +
                    "source partition=" + outrec.sourcePartition() + " " +
                    "source offset=" + outrec.sourceOffset()       
                );
                    
                assertTrue (
                    "Expecting topic: " + mockName + ", got: " + 
                    outrec.topic(),
                    outrec.topic().equals(mockName)
                );
                
                ReplicateOffset repOffset = ReplicateOffset.fromJSONString(
                    (String)
                    outrec.sourceOffset().get(
                        ReplicateSourceTask.REPLICATE_OFFSET_KEY
                    )
                );
                    
                assertTrue (
                    "Expecting stored offset: " + expectedOffsets[i] + ", " +
                    "got: " + repOffset,
                    repOffset.compareTo(expectedOffsets[i]) == 0
                );
                    
                logger.info (
                    "Validating field 0 [XID]: " + 
                    kstruct.get(fields.get(0))
                );
                assertTrue (
                    "Expecting XID: " + inrec.getTransactionId() + ", got: " +
                    kstruct.get(fields.get(0)),
                    kstruct.get(fields.get(0)).equals (inrec.getTransactionId())
                );

                logger.info (
                    "Validating field 1 [TYPE]: " + 
                    kstruct.get(fields.get(1))
                );
                assertTrue (
                    "Expecting TYPE: " + inrec.getAction() + ", got: " +
                    kstruct.get(fields.get(1)),
                    kstruct.get(fields.get(1)).equals (
                        inrec.getAction().toString()
                    )
                );

                logger.info (
                    "Validating field 2 [CHANGE_ID]: " + 
                    kstruct.get(fields.get(2))
                );
                assertTrue (
                    "Expecting CHANGE_ID: " + inrec.getId() + ", got: " +
                    kstruct.get(fields.get(2)),
                    kstruct.get(fields.get(2)).equals (inrec.getId())
                );

                ColumnValue column1 = inrec.getColumnValues().get(0);
                
                logger.info (
                    "Validating field 3 [ID]: " + 
                    kstruct.get(fields.get(3))
                );
                assertTrue (
                    "Expecting column: " + column1.getName() + ", got: " +
                    fields.get(3).name(),
                    fields.get(3).name().equals (column1.getName())
                );
                assertTrue (
                    "Expecting ID: " + column1.getValue() + ", got: " +
                    kstruct.get(fields.get(3)),
                    kstruct.get(fields.get(3)).equals (column1.getValue())
                );

                ColumnValue column2 = inrec.getColumnValues().get(1);
                
                assertTrue (
                    "Expecting column: " + column2.getName() + ", got: " +
                    fields.get(4).name(),
                    fields.get(4).name().equals (column2.getName())
                );
                logger.info (
                    "Validating field 4 [TEST_NAME]: " + 
                    kstruct.get(fields.get(4))
                );
                assertTrue (
                    "Expecting TEST_NAME: " + column2.getValue() + ", got: " +
                    kstruct.get(fields.get(4)),
                    kstruct.get(fields.get(4)).equals (column2.getValue())
                );
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testConvertTransactionInfoRecords() {
        try {
            
            final SchemaBuilder avBuilder = 
                SchemaBuilder.struct().name ("SCHEMA_CHANGE_COUNT");
                    
            avBuilder.field (
                "SCHEMA_NAME",
                Schema.STRING_SCHEMA
            );
                
            avBuilder.field (
                "CHANGE_COUNT",
                Schema.INT32_SCHEMA
            );
                    
            final LinkedHashMap<String, Schema> fieldSchemas =
                new LinkedHashMap<String, Schema>() {{
                    put ("XID", Schema.STRING_SCHEMA);
                    put ("START_SCN", Schema.INT64_SCHEMA);
                    put ("END_SCN", Schema.INT64_SCHEMA);
                    put ("START_TIME", Timestamp.builder().build());
                    put ("END_TIME", Timestamp.builder().build());
                    put ("START_CHANGE_ID", Schema.INT64_SCHEMA);
                    put ("END_CHANGE_ID", Schema.INT64_SCHEMA);
                    put ("CHANGE_COUNT", Schema.INT32_SCHEMA);
                    put (
                        "SCHEMA_CHANGE_COUNT_ARRAY", 
                        SchemaBuilder.array(avBuilder.build()).build()
                    );
            }};
            
            final LinkedList<Field> fields = 
                new LinkedList<Field>() {{
                    add(
                        new Field(
                            "XID", 
                            0, 
                            fieldSchemas.get("XID")
                        )
                    );
                    add(
                        new Field(
                            "START_SCN", 
                            1, 
                            fieldSchemas.get("START_SCN")
                        )
                    );
                    add(
                        new Field(
                            "END_SCN",
                            2,
                            fieldSchemas.get("END_SCN")
                        )
                    );
                    add(
                        new Field(
                            "START_TIME",
                            3,
                            fieldSchemas.get("START_TIME")
                        )
                    );
                    add(
                        new Field(
                            "END_TIME",
                            4,
                            fieldSchemas.get("END_TIME")
                        )
                    );
                    add(
                        new Field(
                            "START_CHANGE_ID",
                            5,
                            fieldSchemas.get("START_CHANGE_ID")
                        )
                    );
                    add(
                        new Field(
                            "END_CHANGE_ID",
                            6,
                            fieldSchemas.get("END_CHANGE_ID")
                        )
                    );
                    add(
                        new Field(
                            "CHANGE_COUNT",
                            7,
                            fieldSchemas.get("CHANGE_COUNT")
                        )
                    );
                    add (
                        new Field(
                            "SCHEMA_CHANGE_COUNT_ARRAY",
                            8,
                            fieldSchemas.get("SCHEMA_CHANGE_COUNT_ARRAY")
                        )
                    );
                }};
                
            final ReplicateOffset [] expectedOffsets = new ReplicateOffset[] {
                PLOG_21_TRANSACTION_OFFSET,
                PLOG_22_TRANSACTION_OFFSET,
                PLOG_23_TRANSACTION_OFFSET
            };
                
            final String mockName = "MOCK-TXMETA"; 
            
            for (int i = 0; i < TRANSACTION_INFO_RECORDS_JSON.length; i++) {
                Schema mockSchema = EasyMock.createMock(Schema.class);
                TopicSchema schema = new TopicSchema (null, mockSchema);
                
                EasyMock.expect (mockSchema.name()).andReturn(mockName);
                EasyMock.expect (mockSchema.type()).andReturn(Schema.Type.STRUCT);
                EasyMock.expect (mockSchema.fields()).andReturn (fields);
                EasyMock.expect (mockSchema.field("XID"))
                    .andReturn(fields.get(0));
                EasyMock.expect (mockSchema.field("START_SCN"))
                    .andReturn(fields.get(1));
                EasyMock.expect (mockSchema.field("END_SCN"))
                    .andReturn(fields.get(2));
                EasyMock.expect (mockSchema.field("START_TIME"))
                    .andReturn(fields.get(3));
                EasyMock.expect (mockSchema.field("END_TIME"))
                    .andReturn(fields.get(4));
                EasyMock.expect (mockSchema.field("START_CHANGE_ID"))
                    .andReturn(fields.get(5));
                EasyMock.expect (mockSchema.field("END_CHANGE_ID"))
                    .andReturn(fields.get(6));
                EasyMock.expect (mockSchema.field("CHANGE_COUNT"))
                    .andReturn(fields.get(7));
                EasyMock.expect (mockSchema.field("SCHEMA_CHANGE_COUNT_ARRAY"))
                    .andReturn(fields.get(8));
                EasyMock.expect (mockSchema.field("SCHEMA_CHANGE_COUNT_ARRAY"))
                    .andReturn(fields.get(8));
                EasyMock.replay (mockSchema);
                
                TransactionInfoRecord inrec = 
                    TransactionInfoRecord.fromJSONString (
                        TRANSACTION_INFO_RECORDS_JSON[i]
                    );
                
                /* we do not serialize/de-serialize replicate offset, it's
                 * redundant and transient
                 */
                inrec.setReplicateOffset(expectedOffsets[i]);
                
                inrec.incrementSchemaRecordCount(EXPECTED_TABLE);
            
                logger.info ("In: " + inrec.toJSONString());
                 
                SourceRecord outrec = 
                    new ReplicateRecordConverter()
                        .schema (schema)
                        .record ((DomainRecord)inrec)
                        .mode(rowTxMetaMode)
                        .convert();
                
                Struct kstruct = (Struct)outrec.value();
                 
                logger.info (
                    "Out: " +
                    "topic=" + outrec.topic()                      + " " +
                    "source partition=" + outrec.sourcePartition() + " " +
                    "source offset=" + outrec.sourceOffset()
                );
                
                assertTrue (
                    "Expecting topic: " + mockName + ", got: " + 
                    outrec.topic(),
                    outrec.topic().equals(mockName)
                );
                
                ReplicateOffset repOffset = ReplicateOffset.fromJSONString(
                    (String)
                    outrec.sourceOffset().get(
                        ReplicateSourceTask.REPLICATE_OFFSET_KEY
                    )
                );
                        
                assertTrue (
                    "Expecting stored offset: " + expectedOffsets[i] + ", " +
                    "got: " + repOffset,
                    repOffset.compareTo(expectedOffsets[i]) == 0
                );
                
                logger.info (
                    "Validating field 0 [XID]: " + 
                    kstruct.get(fields.get(0))
                );
                assertTrue (
                    "Expecting XID: " + inrec.getId() + ", got: " +
                    kstruct.get(fields.get(0)),
                    kstruct.get(fields.get(0)).equals (inrec.getId())
                );
                
                logger.info (
                    "Validating field 1 [START_SCN]: " + 
                    kstruct.get(fields.get(1))
                );
                assertTrue (
                    "Expecting START_SCN: " + inrec.getStartSCN() + 
                    ", got: " + kstruct.get(fields.get(1)),
                    kstruct.get(fields.get(1)).equals (inrec.getStartSCN())
                );
                
                logger.info (
                    "Validating field 2 [END_SCN]: " + 
                    kstruct.get(fields.get(2))
                );
                assertTrue (
                    "Expecting END_SCN: " + inrec.getEndSCN() + 
                    ", got: " + kstruct.get(fields.get(2)),
                    kstruct.get(fields.get(2)).equals (inrec.getEndSCN())
                );
                
                logger.info (
                    "Validating field 3 [START_TIME]: " + 
                    kstruct.get(fields.get(3))
                );
                assertTrue (
                    "Expecting START_TIME: " + inrec.getStartTime() + 
                    ", got: " + kstruct.get(fields.get(3)),
                    kstruct.get(fields.get(3)).equals (inrec.getStartTime())
                );
                
                logger.info (
                    "Validating field 4 [END_TIME]: " + 
                    kstruct.get(fields.get(1))
                );
                assertTrue (
                    "Expecting END_TIME: " + inrec.getEndTime() + 
                    ", got: " + kstruct.get(fields.get(4)),
                    kstruct.get(fields.get(4)).equals (inrec.getEndTime())
                );
                
                logger.info (
                    "Validating field 5 [START_CHANGE_ID]: " + 
                    kstruct.get(fields.get(5))
                );
                assertTrue (
                    "Expecting START_CHANGE_ID: " + inrec.getStartRecordId() +
                    ", got: " + kstruct.get(fields.get(5)),
                    kstruct.get(fields.get(5)).equals (inrec.getStartRecordId())
                );
                
                logger.info (
                    "Validating field 6 [END_CHANGE_ID]: " + 
                    kstruct.get(fields.get(6))
                );
                assertTrue (
                    "Expecting END_CHANGE_ID: " + inrec.getEndRecordId() + 
                    ", got: " + kstruct.get(fields.get(6)),
                    kstruct.get(fields.get(6)).equals (inrec.getEndRecordId())
                );
                
                logger.info (
                    "Validating field 7 [CHANGE_COUNT]: " + 
                    kstruct.get(fields.get(7))
                );
                assertTrue (
                    "Expecting CHANGE_COUNT: " + inrec.getRecordCount() + 
                    ", got: " + kstruct.get(fields.get(7)),
                    kstruct.get(fields.get(7)).equals (inrec.getRecordCount())
                );
                
                logger.info (
                    "Validating field 8 [SCHEMA_CHANGE_COUNT_ARRAY]"
                );
                @SuppressWarnings("unchecked")
                List<Struct> sarray = (List<Struct>)kstruct.get(fields.get(8));
                
                Map<String, Integer> counts = inrec.getSchemaRecordCounts();
                for (Struct cstruct : sarray) {
                    logger.info (
                        "Validating array struct field [SCHEMA_NAME]: " + 
                        cstruct.get("SCHEMA_NAME")
                    );
                    logger.info (
                        "Validating array struct field [CHANGE_COUNT]: " + 
                        cstruct.get("CHANGE_COUNT")
                    );
                    assertTrue (
                        "Expecting: " + counts.get (EXPECTED_TABLE) + " " +
                        "change records for: " + EXPECTED_TABLE,
                        cstruct.get("SCHEMA_NAME").equals (EXPECTED_TABLE) &&
                        cstruct.get("CHANGE_COUNT").equals (
                            counts.get (EXPECTED_TABLE)
                        )
                    );
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testConvertChangeSetRecords() {
        try {
            final Schema keySchema = Schema.STRING_SCHEMA;
            final Schema valueSchema = SchemaBuilder.struct().name("CHANGE_SET")
                .field (
                    "ID", Schema.OPTIONAL_INT32_SCHEMA
                )
                .field (
                    "TEST_NAME", Schema.OPTIONAL_STRING_SCHEMA
                ).build();
            
            final LinkedHashMap<String, Schema> fieldSchemas =
                new LinkedHashMap<String, Schema>() {{
                    put ("XID", Schema.STRING_SCHEMA);
                    put ("TYPE", Schema.STRING_SCHEMA);
                    put ("CHANGE_ID", Schema.INT64_SCHEMA);
                    put ("CHANGE_DATA", SchemaBuilder.map (keySchema, valueSchema));
                }};

            final LinkedList<Field> fields = 
                new LinkedList<Field>() {{
                    add(
                        new Field(
                            "XID", 
                            0, 
                            fieldSchemas.get("XID")
                        )
                    );
                    add(
                        new Field(
                            "TYPE", 
                            1, 
                            fieldSchemas.get("TYPE")
                        )
                    );
                    add(
                        new Field(
                            "CHANGE_ID",
                            2,
                            fieldSchemas.get("CHANGE_ID")
                        )
                    );
                    add(
                        new Field(
                            "CHANGE_DATA",
                            3,
                            fieldSchemas.get("CHANGE_DATA")
                        )
                    );
                }};
            
            final ReplicateOffset [] expectedOffsets = new ReplicateOffset[] {
                PLOG_21_CHANGE_OFFSET,
                PLOG_22_CHANGE_OFFSET,
                PLOG_23_CHANGE_OFFSET
            };
                    
            final String mockName = "MOCK-SOE.UNITTEST"; 
                
            for (int i = 0; i < CHANGESET_RECORDS_JSON.length; i++) {
                Schema mockSchema = EasyMock.createMock(Schema.class);
                TopicSchema schema = new TopicSchema (null, mockSchema);
                
                EasyMock.expect (mockSchema.name())
                    .andReturn(mockName);
                EasyMock.expect (mockSchema.type())
                    .andReturn(Schema.Type.STRUCT);
                EasyMock.expect (mockSchema.fields())
                    .andReturn(fields);
                EasyMock.expect (mockSchema.field("XID"))
                    .andReturn(fields.get(0));
                EasyMock.expect (mockSchema.field("TYPE"))
                    .andReturn(fields.get(1));
                EasyMock.expect (mockSchema.field("CHANGE_ID"))
                    .andReturn(fields.get(2));
                /* expect 4 lookups to replay for mock field CHANGE_DATA 
                 * due to nature of using map of structs as embedded 
                 * field */
                EasyMock.expect (mockSchema.field("CHANGE_DATA"))
                    .andReturn(fields.get(3));
                EasyMock.expect (mockSchema.field("CHANGE_DATA"))
                    .andReturn(fields.get(3));
                EasyMock.expect (mockSchema.field("CHANGE_DATA"))
                    .andReturn(fields.get(3));
                EasyMock.expect (mockSchema.field("CHANGE_DATA"))
                    .andReturn(fields.get(3));
                EasyMock.replay (mockSchema);
                
                ChangeSetRecord inrec = 
                    ChangeSetRecord.fromJSONString(CHANGESET_RECORDS_JSON[i]);
                
                /* we do not serialize/de-serialize replicate offset, it's
                 * redundant and transient
                 */
                inrec.setReplicateOffset(expectedOffsets[i]);
            
                logger.info ("In: " + inrec.toJSONString());
                     
                SourceRecord outrec = 
                    new ReplicateRecordConverter()
                        .schema (schema)
                        .record ((DomainRecord)inrec)
                        .mode(setTxMetaMode)
                        .convert();
                    
                Struct kstruct = (Struct)outrec.value();
                     
                logger.info (
                    "Out: " +
                    "topic=" + outrec.topic()                      + " " +
                    "source partition=" + outrec.sourcePartition() + " " +
                    "source offset=" + outrec.sourceOffset()       
                );
                    
                assertTrue (
                    "Expecting topic: " + mockName + ", got: " + 
                    outrec.topic(),
                    outrec.topic().equals(mockName)
                );
                
                ReplicateOffset repOffset = ReplicateOffset.fromJSONString(
                    (String)
                    outrec.sourceOffset().get(
                        ReplicateSourceTask.REPLICATE_OFFSET_KEY
                    )
                );
                    
                assertTrue (
                    "Expecting stored offset: " + expectedOffsets[i] + ", " +
                    "got: " + repOffset,
                    repOffset.compareTo(expectedOffsets[i]) == 0
                );
                    
                logger.info (
                    "Validating field 0 [XID]: " + 
                    kstruct.get(fields.get(0))
                );
                assertTrue (
                    "Expecting XID: " + inrec.getTransactionId() + ", got: " +
                    kstruct.get(fields.get(0)),
                    kstruct.get(fields.get(0)).equals (inrec.getTransactionId())
                );

                logger.info (
                    "Validating field 1 [TYPE]: " + 
                    kstruct.get(fields.get(1))
                );
                assertTrue (
                    "Expecting TYPE: " + inrec.getAction() + ", got: " +
                    kstruct.get(fields.get(1)),
                    kstruct.get(fields.get(1)).equals (
                        inrec.getAction().toString()
                    )
                );

                logger.info (
                    "Validating field 2 [CHANGE_ID]: " + 
                    kstruct.get(fields.get(2))
                );
                assertTrue (
                    "Expecting CHANGE_ID: " + inrec.getId() + ", got: " +
                    kstruct.get(fields.get(2)),
                    kstruct.get(fields.get(2)).equals (inrec.getId())
                );
                
                Map <String, List<ColumnValue>> inValues =
                    new LinkedHashMap<String, List<ColumnValue>>();
         
                @SuppressWarnings("unchecked")
                LinkedHashMap<String, Struct> lmap = 
                    (LinkedHashMap<String, Struct>)kstruct.get(fields.get(3));
                
                switch (inrec.getAction()) {
                    case INSERT:
                        inValues.put ("NEW", inrec.getNewValues());
                        break;
                    case UPDATE:
                        inValues.put ("KEY", inrec.getKeyValues());
                        /* in this example there is only two fields, no PK,
                         * so both are part of supp key, therefore not
                         * OLD for column TEST_NAME
                         */
                        inValues.put ("NEW", inrec.getNewValues());
                        break;
                    case DELETE:
                        inValues.put ("KEY", inrec.getKeyValues());
                        break;
                    default:
                        break;
                }
                
                for (String type : inValues.keySet()) {
                    Struct mstruct = lmap.get (type);
                    List<ColumnValue> columnValues = inValues.get (type);
                    ColumnValue column1 = columnValues.get(0);
                    
                    logger.info (
                        "Validating field 3 [" + column1.getName() + "]: " +
                        mstruct.get(column1.getName())
                    );
                    assertTrue (
                        "Expecting column: " + column1.getName(),
                        valueSchema.field(column1.getName()).name()
                                   .equals (column1.getName())
                    );
                    assertTrue (
                        "Expecting ID: " + column1.getValue() + ", got: " +
                        mstruct.get(column1.getName()),
                        mstruct.get(column1.getName())
                               .equals (column1.getValue())
                    );
    
                    if (columnValues.size() == 2) {
                        /* second column is not always present for change set
                         * because only one field is updated in UPDATE LCR */
                        ColumnValue column2 = columnValues.get(1);
                        assertTrue (
                            "Expecting column: " + column2.getName(),
                            valueSchema.field(column2.getName()).name()
                                       .equals (column2.getName())
                        );
                        logger.info (
                            "Validating field 4 [" + column2.getName() + "]: " +
                            mstruct.get(column2.getName())
                        );
                        assertTrue (
                            "Expecting TEST_NAME: " + column2.getValue() + ", got: " +
                            mstruct.get(column2.getName()),
                            mstruct.get(column2.getName())
                                   .equals (column2.getValue())
                        );
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    /** 
     * This tests that no transaction fields are published to each topic
     * message when transaction info publishing is disabled in configuration
     */
    @SuppressWarnings("serial")
    @Test
    public void testConvertChangeRowRecordsNoMetaData() {
        try {
            /* only the table fields should be present */
            final LinkedHashMap<String, Schema> fieldSchemas =
                new LinkedHashMap<String, Schema>() {{
                    put ("ID", Schema.INT32_SCHEMA);
                    put ("TEST_NAME", Schema.STRING_SCHEMA);
                }};
                
            final LinkedList<Field> fields = 
                new LinkedList<Field>() {{
                    add(
                        new Field(
                            "ID",
                            0,
                            fieldSchemas.get("ID")
                        )
                    );
                    add(
                        new Field(
                            "TEST_NAME",
                            1,
                            fieldSchemas.get("TEST_NAME")
                        )
                    );
                }};
            
            final ReplicateOffset [] expectedOffsets = new ReplicateOffset[] {
                PLOG_21_CHANGE_OFFSET,
                PLOG_22_CHANGE_OFFSET,
                PLOG_23_CHANGE_OFFSET
            };
                    
            final String mockName = "MOCK-SOE.UNITTEST"; 
                
            for (int i = 0; i < CHANGEROW_RECORDS_JSON.length; i++) {
                Schema mockSchema = EasyMock.createMock(Schema.class);
                TopicSchema schema = new TopicSchema (null, mockSchema);
                
                EasyMock.expect (mockSchema.name())
                    .andReturn(mockName);
                EasyMock.expect (mockSchema.type())
                    .andReturn(Schema.Type.STRUCT);
                EasyMock.expect (mockSchema.fields())
                    .andReturn(fields);
                EasyMock.expect (mockSchema.field("ID"))
                    .andReturn(fields.get(0));
                EasyMock.expect (mockSchema.field("TEST_NAME"))
                    .andReturn(fields.get(1));
                EasyMock.replay (mockSchema);
                
                ChangeRowRecord inrec = 
                    ChangeRowRecord.fromJSONString(CHANGEROW_RECORDS_JSON[i]);
                
                /* we do not serialize/de-serialize replicate offset, it's
                 * redundant and transient
                 */
                inrec.setReplicateOffset(expectedOffsets[i]);
            
                logger.info ("In: " + inrec.toJSONString());
                     
                SourceRecord outrec = 
                    new ReplicateRecordConverter()
                        .schema (schema)
                        .record ((DomainRecord)inrec)
                        .mode(rowNoTxMetaMode)
                        .convert();
                    
                Struct kstruct = (Struct)outrec.value();
                     
                logger.info (
                    "Out: " +
                    "topic=" + outrec.topic()                      + " " +
                    "source partition=" + outrec.sourcePartition() + " " +
                    "source offset=" + outrec.sourceOffset()       
                );
                    
                assertTrue (
                    "Expecting topic: " + mockName + ", got: " + 
                    outrec.topic(),
                    outrec.topic().equals(mockName)
                );
                
                ReplicateOffset repOffset = ReplicateOffset.fromJSONString(
                    (String)
                    outrec.sourceOffset().get(
                        ReplicateSourceTask.REPLICATE_OFFSET_KEY
                    )
                );
                    
                assertTrue (
                    "Expecting stored offset: " + expectedOffsets[i] + ", " +
                    "got: " + repOffset,
                    repOffset.compareTo(expectedOffsets[i]) == 0
                );
                    
                ColumnValue column1 = inrec.getColumnValues().get(0);
                
                logger.info (
                    "Validating field 0 [ID]: " + 
                    kstruct.get(fields.get(0))
                );
                assertTrue (
                    "Expecting column: " + column1.getName() + ", got: " +
                    fields.get(0).name(),
                    fields.get(0).name().equals (column1.getName())
                );
                assertTrue (
                    "Expecting ID: " + column1.getValue() + ", got: " +
                    kstruct.get(fields.get(0)),
                    kstruct.get(fields.get(0)).equals (column1.getValue())
                );

                ColumnValue column2 = inrec.getColumnValues().get(1);
                
                assertTrue (
                    "Expecting column: " + column2.getName() + ", got: " +
                    fields.get(1).name(),
                    fields.get(1).name().equals (column2.getName())
                );
                logger.info (
                    "Validating field 1 [TEST_NAME]: " + 
                    kstruct.get(fields.get(1))
                );
                assertTrue (
                    "Expecting TEST_NAME: " + column2.getValue() + ", got: " +
                    kstruct.get(fields.get(1)),
                    kstruct.get(fields.get(1)).equals (column2.getValue())
                );
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
    
    /** 
     * This tests that no transaction topic message ares published
     * when transaction info publishing is disabled in configuration
     */
    @SuppressWarnings("serial")
    @Test
    public void testConvertTransactionInfoRecordsNoMetaData() {
        try {
            
            final SchemaBuilder avBuilder = 
                SchemaBuilder.struct().name ("SCHEMA_CHANGE_COUNT");
                    
            avBuilder.field (
                "SCHEMA_NAME",
                Schema.STRING_SCHEMA
            );
                
            avBuilder.field (
                "CHANGE_COUNT",
                Schema.INT32_SCHEMA
            );
                    
            final LinkedHashMap<String, Schema> fieldSchemas =
                new LinkedHashMap<String, Schema>() {{
                    put ("XID", Schema.STRING_SCHEMA);
                    put ("START_SCN", Schema.INT64_SCHEMA);
                    put ("END_SCN", Schema.INT64_SCHEMA);
                    put ("START_TIME", Timestamp.builder().build());
                    put ("END_TIME", Timestamp.builder().build());
                    put ("START_CHANGE_ID", Schema.INT64_SCHEMA);
                    put ("END_CHANGE_ID", Schema.INT64_SCHEMA);
                    put ("CHANGE_COUNT", Schema.INT32_SCHEMA);
                    put (
                        "SCHEMA_CHANGE_COUNT_ARRAY", 
                        SchemaBuilder.array(avBuilder.build()).build()
                    );
            }};
            
            final LinkedList<Field> fields = 
                new LinkedList<Field>() {{
                    add(
                        new Field(
                            "XID", 
                            0, 
                            fieldSchemas.get("XID")
                        )
                    );
                    add(
                        new Field(
                            "START_SCN", 
                            1, 
                            fieldSchemas.get("START_SCN")
                        )
                    );
                    add(
                        new Field(
                            "END_SCN",
                            2,
                            fieldSchemas.get("END_SCN")
                        )
                    );
                    add(
                        new Field(
                            "START_TIME",
                            3,
                            fieldSchemas.get("START_TIME")
                        )
                    );
                    add(
                        new Field(
                            "END_TIME",
                            4,
                            fieldSchemas.get("END_TIME")
                        )
                    );
                    add(
                        new Field(
                            "START_CHANGE_ID",
                            5,
                            fieldSchemas.get("START_CHANGE_ID")
                        )
                    );
                    add(
                        new Field(
                            "END_CHANGE_ID",
                            6,
                            fieldSchemas.get("END_CHANGE_ID")
                        )
                    );
                    add(
                        new Field(
                            "CHANGE_COUNT",
                            7,
                            fieldSchemas.get("CHANGE_COUNT")
                        )
                    );
                    add (
                        new Field(
                            "SCHEMA_CHANGE_COUNT_ARRAY",
                            8,
                            fieldSchemas.get("SCHEMA_CHANGE_COUNT_ARRAY")
                        )
                    );
                }};
                
            final String mockName = "MOCK-TXMETA"; 
            
            for (int i = 0; i < TRANSACTION_INFO_RECORDS_JSON.length; i++) {
                Schema mockSchema = EasyMock.createMock(Schema.class);
                TopicSchema schema = new TopicSchema (null, mockSchema);
                
                EasyMock.expect (mockSchema.name()).andReturn(mockName);
                EasyMock.expect (mockSchema.type()).andReturn(Schema.Type.STRUCT);
                EasyMock.expect (mockSchema.fields()).andReturn (fields);
                EasyMock.expect (mockSchema.field("XID"))
                    .andReturn(fields.get(0));
                EasyMock.expect (mockSchema.field("START_SCN"))
                    .andReturn(fields.get(1));
                EasyMock.expect (mockSchema.field("END_SCN"))
                    .andReturn(fields.get(2));
                EasyMock.expect (mockSchema.field("START_TIME"))
                    .andReturn(fields.get(3));
                EasyMock.expect (mockSchema.field("END_TIME"))
                    .andReturn(fields.get(4));
                EasyMock.expect (mockSchema.field("START_CHANGE_ID"))
                    .andReturn(fields.get(5));
                EasyMock.expect (mockSchema.field("END_CHANGE_ID"))
                    .andReturn(fields.get(6));
                EasyMock.expect (mockSchema.field("CHANGE_COUNT"))
                    .andReturn(fields.get(7));
                EasyMock.expect (mockSchema.field("SCHEMA_CHANGE_COUNT_ARRAY"))
                    .andReturn(fields.get(8));
                EasyMock.expect (mockSchema.field("SCHEMA_CHANGE_COUNT_ARRAY"))
                    .andReturn(fields.get(8));
                EasyMock.replay (mockSchema);
                
                TransactionInfoRecord inrec = 
                    TransactionInfoRecord.fromJSONString (
                        TRANSACTION_INFO_RECORDS_JSON[i]
                    );
                
                logger.info ("In: " + inrec.toJSONString());
                 
                new ReplicateRecordConverter()
                    .schema (schema)
                    .record ((DomainRecord)inrec)
                    .mode(rowNoTxMetaMode)
                    .convert();
                
                fail (
                    "An invalid configuration exception should have " +
                    "been thrown"
                );
            }
        } catch (Exception e) {
            assertTrue (
                "Expecting invalid configuration exception",
                e.getMessage().equals(
                    "Invalid configuration, not expecting TRANSACTION INFO " + 
                    "RECORD records when configured to not publish "         +
                    "transaction info meta data messages"
                )
            );
        }
    }

    /** 
     * This tests that no transaction fields are published to each topic
     * message and supplementally logged keys are published when both
     * enabled in configuration
     */
    @SuppressWarnings("serial")
    @Test
    public void testConvertChangeRowRecordsNoMetaDataWithKeys() {
        try {
            /* only the table fields should be present */
            final LinkedHashMap<String, Schema> fieldSchemas =
                new LinkedHashMap<String, Schema>() {{
                    put ("ID", Schema.INT32_SCHEMA);
                    put ("TEST_NAME", Schema.STRING_SCHEMA);
                }};
                
            final LinkedList<Field> keys = 
                new LinkedList<Field>() {{
                    add(
                        new Field(
                            "ID",
                            0,
                            fieldSchemas.get("ID")
                        )
                    );
                    add(
                        new Field(
                            "TEST_NAME",
                            1,
                            fieldSchemas.get("TEST_NAME")
                        )
                    );
            }};
                
            final LinkedList<Field> fields = 
                new LinkedList<Field>() {{
                    add(
                        new Field(
                            "ID",
                            0,
                            fieldSchemas.get("ID")
                        )
                    );
                    add(
                        new Field(
                            "TEST_NAME",
                            1,
                            fieldSchemas.get("TEST_NAME")
                        )
                    );
                }};
            
            final ReplicateOffset [] expectedOffsets = new ReplicateOffset[] {
                PLOG_21_CHANGE_OFFSET,
                PLOG_22_CHANGE_OFFSET,
                PLOG_23_CHANGE_OFFSET
            };
                    
            final String mockName = "MOCK-SOE.UNITTEST"; 
                
            for (int i = 0; i < CHANGEROW_RECORDS_JSON.length; i++) {
                Schema mockKeySchema = EasyMock.createMock(Schema.class);
                Schema mockValueSchema = EasyMock.createMock(Schema.class);
                TopicSchema schema = 
                     new TopicSchema (mockKeySchema, mockValueSchema);
                
                EasyMock.expect (mockValueSchema.name())
                    .andReturn(mockName);
                EasyMock.expect (mockValueSchema.type())
                    .andReturn(Schema.Type.STRUCT);
                EasyMock.expect (mockValueSchema.fields())
                    .andReturn(fields);
                EasyMock.expect (mockValueSchema.field("ID"))
                    .andReturn(fields.get(0));
                EasyMock.expect (mockValueSchema.field("TEST_NAME"))
                    .andReturn(fields.get(1));
                EasyMock.replay (mockValueSchema);
                
                EasyMock.expect (mockKeySchema.name())
                    .andReturn(mockName);
                EasyMock.expect (mockKeySchema.type())
                    .andReturn(Schema.Type.STRUCT);
                EasyMock.expect (mockKeySchema.fields())
                    .andReturn(keys);
                EasyMock.expect (mockKeySchema.fields())
                    .andReturn(keys);
                EasyMock.expect (mockKeySchema.fields())
                    .andReturn(keys);
                EasyMock.expect (mockKeySchema.field("ID"))
                    .andReturn(keys.get(0));
                EasyMock.expect (mockKeySchema.field("TEST_NAME"))
                    .andReturn(keys.get(1));
                /* two mocked lookups expected */
                EasyMock.expect (mockKeySchema.field("ID"))
                    .andReturn(keys.get(0));
                EasyMock.expect (mockKeySchema.field("TEST_NAME"))
                    .andReturn(keys.get(1));
                EasyMock.replay (mockKeySchema);
                
                ChangeRowRecord inrec = 
                    ChangeRowRecord.fromJSONString(CHANGEROW_RECORDS_JSON[i]);
                
                /* we do not serialize/de-serialize replicate offset, it's
                 * redundant and transient
                 */
                inrec.setReplicateOffset(expectedOffsets[i]);
                /* set all columns as key, as it would have been with no PK */
                for (ColumnValue cv : inrec.getColumnValues()) {
                    cv.setIsKeyValue(true);
                }
            
                logger.info ("In: " + inrec.toJSONString());
                     
                SourceRecord outrec = 
                    new ReplicateRecordConverter()
                        .schema (schema)
                        .record ((DomainRecord)inrec)
                        .mode(rowNoTxMetaWithKeysMode)
                        .convert();
                 
                logger.info (
                    "Out: " +
                    "topic=" + outrec.topic()                      + " " +
                    "source partition=" + outrec.sourcePartition() + " " +
                    "source offset=" + outrec.sourceOffset()       
                );
                    
                assertTrue (
                    "Expecting topic: " + mockName + ", got: " + 
                    outrec.topic(),
                    outrec.topic().equals(mockName)
                );
                
                ReplicateOffset repOffset = ReplicateOffset.fromJSONString(
                    (String)
                    outrec.sourceOffset().get(
                        ReplicateSourceTask.REPLICATE_OFFSET_KEY
                    )
                );
                    
                assertTrue (
                    "Expecting stored offset: " + expectedOffsets[i] + ", " +
                    "got: " + repOffset,
                    repOffset.compareTo(expectedOffsets[i]) == 0
                );
                
                /* key and value */
                Struct kstruct = (Struct)outrec.key();
                Struct vstruct = (Struct)outrec.value();
                
                assertTrue (
                    "Expecting " + keys.size() + " keys, got: " +
                    kstruct.schema().fields().size(),
                    kstruct.schema().fields().size() == keys.size()
                );
                    
                /* validate both key and value structs, in the test fixture
                 * there is not defined PK, so all columns are used
                 * as key, as supplementally logged by source system
                 */
                ColumnValue column1 = inrec.getColumnValues().get(0);

                logger.info (
                    "Validating key 0 [ID]: " + 
                    kstruct.get(keys.get(0))
                );
                assertTrue (
                    "Expecting column: " + column1.getName() + ", got: " +
                    keys.get(0).name(),
                    keys.get(0).name().equals (column1.getName())
                );
                assertTrue (
                    "Expecting ID: " + column1.getValue() + ", got: " +
                    kstruct.get(keys.get(0)),
                    kstruct.get(keys.get(0)).equals (column1.getValue())
                );

                logger.info (
                    "Validating field 0 [ID]: " + 
                    vstruct.get(fields.get(0))
                );
                assertTrue (
                    "Expecting column: " + column1.getName() + ", got: " +
                    fields.get(0).name(),
                    fields.get(0).name().equals (column1.getName())
                );
                assertTrue (
                    "Expecting ID: " + column1.getValue() + ", got: " +
                    vstruct.get(fields.get(0)),
                    vstruct.get(fields.get(0)).equals (column1.getValue())
                );
                
                ColumnValue column2 = inrec.getColumnValues().get(1);
                
                logger.info (
                    "Validating key 1 [TEST_NAME]: " + 
                    kstruct.get(keys.get(1))
                );
                assertTrue (
                    "Expecting column: " + column2.getName() + ", got: " +
                    keys.get(1).name(),
                    keys.get(1).name().equals (column2.getName())
                );
                assertTrue (
                    "Expecting TEST_NAME: " + column2.getValue() + ", got: " +
                    kstruct.get(keys.get(1)),
                    kstruct.get(keys.get(1)).equals (column2.getValue())
                );
                    
                logger.info (
                    "Validating field 1 [TEST_NAME]: " + 
                    vstruct.get(fields.get(1))
                );
                assertTrue (
                    "Expecting column: " + column2.getName() + ", got: " +
                    fields.get(1).name(),
                    fields.get(1).name().equals (column2.getName())
                );
                assertTrue (
                    "Expecting TEST_NAME: " + column2.getValue() + ", got: " +
                    vstruct.get(fields.get(1)),
                    vstruct.get(fields.get(1)).equals (column2.getValue())
                );
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail (e.getMessage());
        }
    }
}
