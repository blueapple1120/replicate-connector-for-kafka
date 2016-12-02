package com.dbvisit.replicate.kafkaconnect;

import static org.junit.Assert.fail;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dbvisit.replicate.plog.config.PlogConfigType;
import com.dbvisit.replicate.plog.domain.ReplicateInfo;
import com.dbvisit.replicate.plog.domain.ReplicateOffset;
import com.dbvisit.replicate.plog.file.PlogFile;

/** Provide configuration and text fixtures for small PLOG set */
public class ReplicateTestConfig {
    protected final int EXPECTED_REP_SIZE = 2;
    protected final int EXPECTED_PLOG_ID = 20;
    protected final int EXPECTED_PLOG_TIMESTAMP = 1468812543;
    protected final long EXPECTED_DATA_OFFSET = 12812;
    
    protected final String EXPECTED_AGGREGATE = "TXMETA";
    protected final ReplicateInfo EXPECTED_AGGREGATE_INFO = new ReplicateInfo ()
    {{
        setPlogUID(
            PlogFile.createPlogUID(
                EXPECTED_PLOG_ID,
                EXPECTED_PLOG_TIMESTAMP
            )
        );
        setDataOffset(EXPECTED_DATA_OFFSET);
        setIdentifier(EXPECTED_AGGREGATE);
        setAggregate(true);
    }};
    protected final String EXPECTED_TABLE = "SOE.UNITTEST";
    protected final ReplicateInfo EXPECTED_TABLE_INFO = new ReplicateInfo() 
    {{
        setPlogUID(
            PlogFile.createPlogUID(
                EXPECTED_PLOG_ID,
                EXPECTED_PLOG_TIMESTAMP
            )
        );
        setDataOffset(EXPECTED_DATA_OFFSET);
        setIdentifier(EXPECTED_TABLE);
    }};
    protected final int EXPECTED_NUM_MESSAGES_PER_PLOG = 2;
    protected String [] EXPECTED_TOPICS = {
        EXPECTED_TABLE,
        EXPECTED_AGGREGATE
    };
    
    public Map<String, String> getConfigPropsForMultiSet () {
        Map<String, String> configProps = new HashMap<String, String> ();
        
        URL resURL = this.getClass().getResource("/data/mine/plog_multi_set");
        
        if (resURL == null) {
            fail ("Mine test path resource is not setup correctly");
        }

        configProps.put (
            PlogConfigType.PLOG_LOCATION_URI.getProperty(), 
            resURL.toString()
        );
        
        /* quit = total scans / health check scans
         * 1 scan interval is 500ms, default 10 scans for health check
         * therefore 2 * 10 * 500 = 10000 ms */
        configProps.put (
            PlogConfigType.SCAN_QUIT_INTERVAL_COUNT.getProperty(),
            "2"
        );

        /* flush after every record */
        configProps.put (
            PlogConfigType.DATA_FLUSH_SIZE.getProperty(),
            "10"
        );
        
        return configProps;
    }
    
    public Map<String, String> getConfigPropsForSourceTasks () {
        Map <String, String> configProps = getConfigPropsForMultiSet();
        
        /* test PLOG data contains one replicated table and it's
         * transaction meta data 
         */
        configProps.put (
            ReplicateSourceTaskConfig.REPLICATED_CONFIG,
            "{" +
            "\"plogUID\":87368158463," +
            "\"dataOffset\":12812," +
            "\"identifier\":\"SOE.UNITTEST\"," +
            "\"aggregate\":false" +
            "}" +
            ReplicateSourceConnector.REPLICATED_GROUP_DELIMTER +
            "{" +
            "\"plogUID\":87368158463," +
            "\"dataOffset\":12812," +
            "\"identifier\":\"TXMETA\"," +
            "\"aggregate\":true" +
            "}" 
        );
        
        return configProps;
    }
    
    public Map<String, String> getConfigPropsForSourceTask () {
        Map <String, String> configProps = getConfigPropsForMultiSet();
        
        /* test PLOG data contains one replicated table and it's
         * transaction meta data 
         */
        configProps.put (
            ReplicateSourceTaskConfig.REPLICATED_CONFIG,
            "{" +
            "\"plogUID\":87368158463," +
            "\"dataOffset\":12812," +
            "\"identifier\":\"TXMETA\"," +
            "\"aggregate\":true" +
            "}" +
            ReplicateSourceConnector.REPLICATED_GROUP_DELIMTER +
            "{" +
            "\"plogUID\":87368158463," +
            "\"dataOffset\":12812," +
            "\"identifier\":\"SOE.UNITTEST\"," +
            "\"aggregate\":false" +
            "}"
        );
        
        return configProps;
    }
    
    public Map<String, String> getConfigPropsForSourceTaskWithStartSCN (
        Long startSCN
    ) {
        Map <String, String> configProps = getConfigPropsForSourceTask ();
        
        configProps.put (
            ReplicateSourceTaskConfig.GLOBAL_SCN_COLD_START_CONFIG,
            startSCN.toString()
        );
        
        return configProps;
    }
    
    @SuppressWarnings("serial")
    protected final List<Map<String, String>> offsetPartitions() {
        return new ArrayList<Map<String, String>>() {{
            add (
                new HashMap<String, String>() {{
                    put (
                        ReplicateSourceTask.REPLICATE_NAME_KEY,
                        EXPECTED_AGGREGATE
                    );
                }}
            );
            add (
                new HashMap<String, String>() {{
                    put (
                        ReplicateSourceTask.REPLICATE_NAME_KEY,
                        EXPECTED_TABLE
                    );
                }}
            );
        }};
    }
    
    /** Create dummy stored offsets for UNIT test table and TX info
     * 
     * @param storedOffset the offset to start processing from
     */ 
    @SuppressWarnings("serial")
    protected final Map<Map<String, String>, Map<String, Object>> storedOffsets (
        final String storedOffset
    ) {
        return
        new HashMap<Map<String, String>, Map<String, Object>>() {{
            put (
                new HashMap<String, String>() {{
                    put (
                        ReplicateSourceTask.REPLICATE_NAME_KEY,
                        EXPECTED_AGGREGATE
                    );
                }},
                new HashMap<String, Object>() {{
                    put (
                        ReplicateSourceTask.REPLICATE_OFFSET_KEY,
                        storedOffset
                    );
                }}
            );
            put (
                new HashMap<String, String>() {{
                    put (
                        ReplicateSourceTask.REPLICATE_NAME_KEY,
                        EXPECTED_TABLE
                    );
                }},
                new HashMap<String, Object>() {{
                    put (
                        ReplicateSourceTask.REPLICATE_OFFSET_KEY,
                        storedOffset
                    );
                }}
            );
        }};
    }
 
    
    /** End offsets as stored for change records in each PLOG */
    /* 90194315216L */
     protected final ReplicateOffset PLOG_21_CHANGE_OFFSET = 
        new ReplicateOffset (
            PlogFile.createPlogUID(21, 1468812547),
            2000L
        );
    /* 94489282120L */
    protected final ReplicateOffset PLOG_22_CHANGE_OFFSET = 
        new ReplicateOffset (
            PlogFile.createPlogUID(22, 1468812549),
            1608L
        );
    /* 98784249300L */
    protected final ReplicateOffset PLOG_23_CHANGE_OFFSET =
        new ReplicateOffset (
            PlogFile.createPlogUID(23, 1468812553),
            1492L
        );
    
    /** These are the end offsets as stored, i.e where the next transaction
     * would have started in PLOG 21
     */
    
    /* 90194315432L */
    protected final ReplicateOffset PLOG_21_TRANSACTION_OFFSET =
        new ReplicateOffset (
            PlogFile.createPlogUID(21, 1468812547),
            2216
        );
    /* 94489282336L */
    protected final ReplicateOffset PLOG_22_TRANSACTION_OFFSET =
        new ReplicateOffset (
            PlogFile.createPlogUID(22, 1468812549),
            1824L
        );
    /* 98784249516L */
    protected final ReplicateOffset PLOG_23_TRANSACTION_OFFSET =
        new ReplicateOffset (
            PlogFile.createPlogUID(23, 1468812553),
            1708L
        );
    
    /** Start SCNs for transactions in PLOG */
    final protected long PLOG_21_TRANSACTION_SCN = 754222L;
    final protected long PLOG_22_TRANSACTION_SCN = 754354L;
    final protected long PLOG_23_TRANSACTION_SCN = 754492L;
    
    /* LCR contents of PLOG set as JSON */
    final protected String[] CHANGE_RECORDS_JSON = new String[] {
        "{"                                            +
            "\"recordType\":\"CHANGE_RECORD\","        +
            "\"action\":\"INSERT\","                   +
            "\"id\":21010000011,"                      +
            "\"plogId\":21,"                           +
            "\"transactionId\":\"0007.00b.000002a2\"," +
            "\"systemChangeNumber\":754222,"           +
            "\"timestamp\":1468769345000,"             +
            "\"tableId\":20253,"                       +
            "\"tableOwner\":\"SOE\","                  +
            "\"tableName\":\"UNITTEST\","              +
            "\"columnValues\":["                       +
                "{"                                    +
                    "\"id\":1,"                        +
                    "\"type\":\"NUMBER\","             +
                    "\"name\":\"ID\","                 +
                    "\"value\":1"                      +
                "},"                                   +
                "{"                                    +
                    "\"id\":2,"                        +
                    "\"type\":\"VARCHAR2\","           +
                    "\"name\":\"TEST_NAME\","          +
                    "\"value\":\"TEST INSERT\""        +
                "}"                                    +
            "],"                                       +
            "\"multiPart\":false,"                     +
            "\"replicateOffset\":{"                    +
            "\"plogUID\":91663125763,"                 +
            "\"plogOffset\":2000"                      +
            "}"                                        +
        "}",
        "{"                                            +
            "\"recordType\":\"CHANGE_RECORD\","        +
            "\"action\":\"UPDATE\","                   +
            "\"id\":22010000008,"                      +
            "\"plogId\":22,"                           +
            "\"transactionId\":\"0003.015.000003bb\"," +
            "\"systemChangeNumber\":754354,"           +
            "\"timestamp\":1468769348000,"             +
            "\"tableId\":20253,"                       +
            "\"tableOwner\":\"SOE\","                  +
            "\"tableName\":\"UNITTEST\","              +
            "\"columnValues\":["                       +
                "{"                                    +
                    "\"id\":1,"                        +
                    "\"type\":\"NUMBER\","             +
                    "\"name\":\"ID\","                 +
                    "\"value\":1"                      +
                "},"                                   +
                "{"                                    +
                    "\"id\":2,"                        +
                    "\"type\":\"VARCHAR2\","           +
                    "\"name\":\"TEST_NAME\","          +
                    "\"value\":\"TEST UPDATE\""        +
                "}"                                    +
            "],"                                       +
            "\"multiPart\":false,"                     +
            "\"replicateOffset\":{"                    +
            "\"plogUID\":95958093061,"                 +
            "\"plogOffset\":1608"                      +
            "}"                                        +
        "}",
        "{"                                            +
            "\"recordType\":\"CHANGE_RECORD\","        +
            "\"action\":\"DELETE\","                   +
            "\"id\":23010000008,"                      +
            "\"plogId\":23,"                           +
            "\"transactionId\":\"0005.007.00000395\"," +
            "\"systemChangeNumber\":754492,"           +
            "\"timestamp\":1468769351000,"             +
            "\"tableId\":20253,"                       +
            "\"tableOwner\":\"SOE\","                  +
            "\"tableName\":\"UNITTEST\","              +
            "\"columnValues\":["                       +
                "{"                                    +
                    "\"id\":1,"                        +
                    "\"type\":\"NUMBER\","             +
                    "\"name\":\"ID\","                 +
                    "\"value\":1"                      +
                "},"                                   +
                "{"                                    +
                    "\"id\":2,"                        +
                    "\"type\":\"VARCHAR2\","           +
                    "\"name\":\"TEST_NAME\","          +
                    "\"value\":\"TEST UPDATE\""        +
                "}"                                    +
            "],"                                       +
            "\"multiPart\":false,"                     +
            "\"replicateOffset\":{"                    +
            "\"plogUID\":100253060361,"                +
            "\"plogOffset\":1492"                      +
            "}"                                        +
        "}"
    };

    /* Meta data contents of PLOG set */
    final protected String METADATA_RECORD_1_JSON =
        "{"                                             +
            "\"recordType\":\"METADATA_RECORD\","       +
            "\"id\":20010000079,"                       +
            "\"plogId\":20,"                            +
            "\"scn\":754108,"                           +
            "\"metaData\":{"                            +
                "\"validSinceSCN\":754107,"             +
                "\"objectId\":20253,"                   +
                "\"schemaName\":\"SOE\","               +
                "\"tableName\":\"UNITTEST\","           +
                "\"valid\":true,"                       +
                "\"schemataName\":\"SOE.UNITTEST\","    +
                "\"columns\":["                         +
                    "{"                                 +
                        "\"columnId\":1,"               +
                        "\"columnName\":\"ID\","        +
                        "\"columnType\":\"NUMBER\","    +
                        "\"columnPrecision\":6,"        +
                        "\"columnScale\":0,"            +
                        "\"isNullable\":false"          +
                    "},"                                +
                    "{"                                 +
                        "\"columnId\":2,"               +
                        "\"columnName\":\"TEST_NAME\"," +
                        "\"columnType\":\"VARCHAR2\","  +
                        "\"columnPrecision\":-1,"       +
                        "\"columnScale\":-1,"           +
                        "\"isNullable\":true"           +
                    "}"                                 +
                "]"                                     +
            "},"                                        +
            "\"replicateOffset\":{"                     +
            "\"plogUID\":87368158463,"                  +
            "\"plogOffset\":13300"                      +
            "}"                                         +
        "}";
    
    /* Meta data contents of PLOG set */
    final protected String METADATA_RECORD_2_JSON =
        "{"                                             +
            "\"recordType\":\"METADATA_RECORD\","       +
            "\"id\":20010000080,"                       +
            "\"plogId\":21,"                            +
            "\"scn\":755108,"                           +
            "\"metaData\":{"                            +
                "\"validSinceSCN\":755107,"             +
                "\"objectId\":20253,"                   +
                "\"schemaName\":\"SOE\","               +
                "\"tableName\":\"UNITTEST\","           +
                "\"valid\":true,"                       +
                "\"schemataName\":\"SOE.UNITTEST\","    +
                "\"columns\":["                         +
                    "{"                                 +
                        "\"columnId\":1,"               +
                        "\"columnName\":\"ID\","        +
                        "\"columnType\":\"NUMBER\","    +
                        "\"columnPrecision\":6,"        +
                        "\"columnScale\":0,"            +
                        "\"isNullable\":false"          +
                    "},"                                +
                    "{"                                 +
                        "\"columnId\":2,"               +
                        "\"columnName\":\"TEST_NAME\"," +
                        "\"columnType\":\"VARCHAR2\","  +
                        "\"columnPrecision\":-1,"       +
                        "\"columnScale\":-1,"           +
                        "\"isNullable\":true"           +
                    "},"                                +
                    "{"                                 +
                        "\"columnId\":3,"               +
                        "\"columnName\":\"SCORE\","     +
                        "\"columnType\":\"NUMBER\","    +
                        "\"columnPrecision\":20,"       +
                        "\"columnScale\":4,"            +
                        "\"isNullable\":false"          +
                    "}"                                 +
                "]"                                     +
            "},"                                        +
            "\"replicateOffset\":{"                     +
            "\"plogUID\":91663125763,"                  +
            "\"plogOffset\":13300"                      +
            "}"                                         +
        "}";
    
    /* TX info contents for PLOG set */
    final protected String[] TRANSACTION_INFO_RECORDS_JSON = new String[] {
        "{"                                               +
            "\"recordType\":\"TRANSACTION_INFO_RECORD\"," +
            "\"id\":\"0007.00b.000002a2\","               +
            "\"startPlogId\":21,"                         +
            "\"endPlogId\":21,"                           +
            "\"startSCN\":754222,"                        +
            "\"endSCN\":754223,"                          +
            "\"startTime\":1468769345000,"                +
            "\"endTime\":1468769345000,"                  +
            "\"startRecordId\":21010000004,"              +
            "\"endRecordId\":21010000012,"                +
            "\"recordCount\":1,"                          +
            "\"replicateOffset\":{"                       +
            "\"plogUID\":91663125763,"                    +
            "\"plogOffset\":2216"                         +
            "}"                                           +
        "}",
        "{"                                               +
            "\"recordType\":\"TRANSACTION_INFO_RECORD\"," +
            "\"id\":\"0003.015.000003bb\","               +
            "\"startPlogId\":22,"                         +
            "\"endPlogId\":22,"                           +
            "\"startSCN\":754354,"                        +
            "\"endSCN\":754355,"                          +
            "\"startTime\":1468769348000,"                +
            "\"endTime\":1468769348000,"                  +
            "\"startRecordId\":22010000001,"              +
            "\"endRecordId\":22010000009,"                +
            "\"recordCount\":1,"                          +
            "\"replicateOffset\":{"                       +
            "\"plogUID\":95958093061,"                    +
            "\"plogOffset\":1824"                         +
            "}"                                           +
        "}",
        "{"                                               +
            "\"recordType\":\"TRANSACTION_INFO_RECORD\"," +
            "\"id\":\"0005.007.00000395\","               +
            "\"startPlogId\":23,"                         +
            "\"endPlogId\":23,"                           +
            "\"startSCN\":754492,"                        +
            "\"endSCN\":754493,"                          +
            "\"startTime\":1468769351000,"                +
            "\"endTime\":1468769351000,"                  +
            "\"startRecordId\":23010000001,"              +
            "\"endRecordId\":23010000009,"                +
            "\"recordCount\":1,"                          +
            "\"replicateOffset\":{"                       +
            "\"plogUID\":100253060361,"                   +
            "\"plogOffset\":1708"                         +
            "}"                                           +  
        "}"
    };
    
    public Map<String, String> getConfigPropsForSourceTaskCustom () {
        Map <String, String> configProps = getConfigPropsForMultiSet();
        
        /* only read LCRs, not transaction info
         */
        configProps.put (
            ReplicateSourceTaskConfig.REPLICATED_CONFIG,
            "{" +
            "\"plogId\":20," +
            "\"dataOffset\":12812," +
            "\"identifier\":\"SOE.UNITTEST\"," +
            "\"aggregate\":false" +
            "}"
        );
        
        return configProps;
    }
    
    /* LCR with no column value as JSON, test to add column value */
    protected String CHANGE_RECORD_NO_VALUES_JSON =
        "{"                                            +
            "\"recordType\":\"CHANGE_RECORD\","        +
            "\"action\":\"INSERT\","                   +
            "\"id\":21010000011,"                      +
            "\"plogId\":21,"                           +
            "\"transactionId\":\"0007.00b.000002a2\"," +
            "\"systemChangeNumber\":754222,"           +
            "\"timestamp\":1468769345000,"             +
            "\"tableId\":20253,"                       +
            "\"tableOwner\":\"SOE\","                  +
            "\"tableName\":\"UNITTEST\","              +
            "\"columnValues\":["                       +
            "],"                                       +
            "\"multiPart\":false,"                     +
            "\"replicateOffset\":{"                    +
            "\"plogUID\":91663125763,"                 +
            "\"plogOffset\":112"                       +
            "}"                                        +
        "}";
            
    /* Empty meta data record, add columns during test */
    protected String METADATA_RECORD_NO_COLUMNS_JSON =
        "{"                                             +
            "\"recordType\":\"METADATA_RECORD\","       +
            "\"id\":20010000079,"                       +
            "\"plogId\":20,"                            +
            "\"scn\":754108,"                           +
            "\"metaData\":{"                            +
                "\"validSinceSCN\":754107,"             +
                "\"objectId\":20253,"                   +
                "\"schemaName\":\"SOE\","               +
                "\"tableName\":\"UNITTEST\","           +
                "\"valid\":true,"                       +
                "\"schemataName\":\"SOE.UNITTEST\","    +
                "\"columns\":["                         +
                "]"                                     +
            "},"                                        +
            "\"replicateOffset\":{"                     +
            "\"plogUID\":87368158463,"                  +
            "\"plogOffset\":13300"                      +
            "}"                                         +
        "}";
}
