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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * Replicate source connector configuration
 */
public class ReplicateSourceConnectorConfig extends AbstractConfig {
    /** PLOG location configuration property, location must be shared
     *  by all source tasks
     */
    public static final String PLOG_LOCATION_CONFIG = "plog.location.uri";
    /** Default location for mined Oracle REDOs */
    public static final String PLOG_LOCATION_DEFAULT =
        "/home/oracle/ktest/mine";
    /** Describe location property */
    private static final String PLOG_LOCATION_DOC =
        "Shared directory that holds the replicate PLOG files produced " +
        "by the Dbvisit Redo Reader. This location must be accessible " +
        "by all worker nodes";
    /** Unused display name for configuration variable, had to downgrade 
     *  config from v0.10 to support v0.9
     */
    @SuppressWarnings("unused")
    private static final String PLOG_LOCATION_DISPLAY = "PLOG Source Location";
    
    /** PLOG data cache flush size configuration property */
    public static final String PLOG_DATA_FLUSH_CONFIG = "plog.data.flush.size";
    /** PLOG data cache flush size default */
    public static final String PLOG_DATA_FLUSH_DEFAULT = "1000";
    /** Describe the flush size configuration property */
    private static final String PLOG_DATA_FLUSH_DOC = 
        "The number of LCRs to cache before flushing, for connector this is " +
        "the batch size";
    /** Unused display name for configuration variable */
    @SuppressWarnings("unused")
    private static final String PLOG_DATA_FLUSH_DISPLAY =
        "PLOG Batch/Flush size";

    /** PLOG wait time interval configuration property */
    public static final String PLOG_INTERVAL_TIME_MS_CONFIG =
        "plog.interval.time.ms";
    /** PLOG wait time interval default in milliseconds */
    public static final String PLOG_INTERVAL_TIME_MS_DEFAULT = "500";
    /** Describe PLOG wait time interval property string */
    private static final String PLOG_INTERVAL_TIME_MS_DOC =
        "Time in milliseconds of one wait interval, used by scans and " +
        "health check";
    /** Unused display name for configuration variable */
    @SuppressWarnings("unused")
    private static final String PLOG_INTERVAL_TIME_MS_DISPLAY =
        "Interval/Wait time ms";

    /** PLOG scan interval count configuration property */
    public static final String PLOG_SCAN_INTERVAL_COUNT_CONFIG =
        "plog.scan.interval.count";
    /**  PLOG scan interval count default */
    public static final String PLOG_SCAN_INTERVAL_COUNT_DEFAULT = "5";
    /** Describe PLOG scan interval count property */
    private static final String PLOG_SCAN_INTERVAL_COUNT_DOC =
        "The number of intervals to wait between scans";
    /** Unused display name for configuration variable */
    @SuppressWarnings("unused")
    private static final String PLOG_SCAN_INTERVAL_COUNT_DISPLAY =
        "Scan Interval Count";
 
    /** PLOG health check count configuration property */
    public static final String PLOG_HEALTH_CHECK_INTERVAL_CONFIG =
        "plog.health.check.interval";
    /** PLOG health check count defaul value */
    public static final String PLOG_HEALTH_CHECK_INTERVAL_DEFAULT = "10";
    /** Describe PLOG health check count variable */
    private static final String PLOG_HEALTH_CHECK_INTERVAL_DOC =
        "The number of intervals between health checks";
    /** Unused display name for configuration variable */
    @SuppressWarnings("unused")
    private static final String PLOG_HEALTH_CHECK_INTERVAL_DISPLAY =
        "Health Check Interval Count";
 
    /** PLOG scan offline interval count configuration for a time out */
    public static final String PLOG_SCAN_OFFLINE_INTERVAL_CONFIG =
        "plog.scan.offline.interval";
    /** PLOG scan offline interval count default */
    public static final String PLOG_SCAN_OFFLINE_INTERVAL_DEFAULT = "100";
    /** Describe PLOG scan offline interval configuration property */
    private static final String PLOG_SCAN_OFFLINE_INTERVAL_DOC =
        "The number of health check scans to use for deciding whether or " +
        "not replicate is offline";
    /** Unused display name for configuration variable */
    @SuppressWarnings("unused")
    private static final String PLOG_SCAN_OFFLINE_INTERVAL_DISPLAY =
        "Scan Offline Check Interval";
 
    /** Topic prefix configuration property */ 
    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    /** Description of prefix configuration property */
    private static final String TOPIC_PREFIX_DOC =
        "Prefix to prepend to table names to generate the name of the " +
        "Kafka topic to publish data to";
    /** Unused display name for configuration variable */
    @SuppressWarnings("unused")
    private static final String TOPIC_PREFIX_DISPLAY = "Topic Prefix";
    /** Use null as default for topic prefix */
    private static final String TOPIC_PREFIX_DEFAULT = null;
    
    /** Transaction meta data topic configuration property */ 
    public static final String TOPIC_NAME_TRANSACTION_INFO_CONFIG = 
        "topic.name.transaction.info";
    /** Description of transaction info topic configuration property */
    private static final String TOPIC_NAME_TRANSACTION_INFO_DOC =
        "Name of the topic to which transaction meta data should be " +
        "published to";
    /** Default for transaction info topic */
    public static final String TOPIC_NAME_TRANSACTION_INFO_DEFAULT = "TXMETA";
    
    /** User specified global SCN to start processing or loading records from */
    public static final String GLOBAL_SCN_COLD_START_CONFIG =
        "plog.global.scn.cold.start";
    /** Description for SCN filter configuration property */
    private static final String GLOBAL_SCN_COLD_START_DOC = 
        "Global SCN to use start prcessing or loading records from, for a " +
        "cold start, eg. no data published to topic yet";
    /** Use null as default for SCN filter */
    private static final Long GLOBAL_SCN_COLD_START_DEFAULT = null;
    
    /**
     * Create basic configuration definition
     * 
     * @return Kafka source connector configuration definition
     */
    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
            .define (
                PLOG_LOCATION_CONFIG, 
                Type.STRING, 
                PLOG_LOCATION_DEFAULT,
                Importance.HIGH, 
                PLOG_LOCATION_DOC
            )
            .define (
                PLOG_DATA_FLUSH_CONFIG, 
                Type.INT,
                PLOG_DATA_FLUSH_DEFAULT,
                Importance.LOW, 
                PLOG_DATA_FLUSH_DOC
            )
            .define (
                PLOG_INTERVAL_TIME_MS_CONFIG, 
                Type.INT,
                PLOG_INTERVAL_TIME_MS_DEFAULT,
                Importance.LOW, 
                PLOG_INTERVAL_TIME_MS_DOC 
            )
            .define (
                PLOG_SCAN_INTERVAL_COUNT_CONFIG, 
                Type.INT,
                PLOG_SCAN_INTERVAL_COUNT_DEFAULT,
                Importance.LOW, 
                PLOG_SCAN_INTERVAL_COUNT_DOC
            )
            .define (
                PLOG_HEALTH_CHECK_INTERVAL_CONFIG, 
                Type.INT,
                PLOG_HEALTH_CHECK_INTERVAL_DEFAULT,
                Importance.LOW, 
                PLOG_HEALTH_CHECK_INTERVAL_DOC 
            )
            .define (
                PLOG_SCAN_OFFLINE_INTERVAL_CONFIG, 
                Type.INT,
                PLOG_SCAN_OFFLINE_INTERVAL_DEFAULT,
                Importance.LOW, 
                PLOG_SCAN_OFFLINE_INTERVAL_DOC 
            )
            .define (
                TOPIC_PREFIX_CONFIG, 
                Type.STRING, 
                TOPIC_PREFIX_DEFAULT,
                Importance.LOW, 
                TOPIC_PREFIX_DOC
            )
            .define (
                TOPIC_NAME_TRANSACTION_INFO_CONFIG,
                Type.STRING,
                TOPIC_NAME_TRANSACTION_INFO_DEFAULT,
                Importance.LOW,
                TOPIC_NAME_TRANSACTION_INFO_DOC
            )
            .define (
                GLOBAL_SCN_COLD_START_CONFIG,
                Type.LONG,
                GLOBAL_SCN_COLD_START_DEFAULT,
                Importance.LOW,
                GLOBAL_SCN_COLD_START_DOC
            );
    }
    static ConfigDef config = baseConfigDef();
    
    /**
     * Construct replicate source connector configuration from property map
     * 
     * @param props Property map
     */
    public ReplicateSourceConnectorConfig (Map<String, String> props) {
        super(config, props);
    }

    /**
     * Construct replicate source connector configuration from configuration
     * definition and property map
     * 
     * @param def   Configuration definition
     * @param props Property map
     */
    protected ReplicateSourceConnectorConfig (
        ConfigDef def, 
        Map<String, String> props
    ) {
        super(def, props);
    }

}
