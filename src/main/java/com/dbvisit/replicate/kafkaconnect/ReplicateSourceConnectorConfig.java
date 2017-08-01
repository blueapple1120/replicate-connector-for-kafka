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
import org.apache.kafka.common.config.ConfigDef.Width;

import com.dbvisit.replicate.kafkaconnect.util.ReplicateInfoTopic;

/**
 * Replicate source connector configuration
 */
public class ReplicateSourceConnectorConfig extends AbstractConfig {
    /** PLOG location configuration property, location must be shared
     *  by all source tasks
     */
    public static final String PLOG_LOCATION_CONFIG = "plog.location.uri";
    /** Default location for mined Oracle REDOs */
    private static final String PLOG_LOCATION_DEFAULT =
        "/home/oracle/ktest/mine";
    /** Describe location property */
    private static final String PLOG_LOCATION_DOC =
        "Shared directory that holds the replicate PLOG files produced " +
        "by the Dbvisit Redo Reader. This location must be accessible " +
        "by all worker nodes";
    /** Display name for location property */
    private static final String PLOG_LOCATION_DISPLAY = "PLOG Source Location";
    
    /** PLOG data cache flush size configuration property */
    public static final String PLOG_DATA_FLUSH_CONFIG = "plog.data.flush.size";
    /** PLOG data cache flush size default */
    private static final Integer PLOG_DATA_FLUSH_DEFAULT = 1000;
    /** Describe the flush size configuration property */
    private static final String PLOG_DATA_FLUSH_DOC = 
        "The number of LCRs to cache before flushing, for connector this is " +
        "the batch size";
    /** Display name for flush size configuration parameter */
    private static final String PLOG_DATA_FLUSH_DISPLAY =
        "PLOG Batch/Flush size";

    /** PLOG wait time interval configuration property */
    public static final String PLOG_INTERVAL_TIME_MS_CONFIG =
        "plog.interval.time.ms";
    /** PLOG wait time interval default in milliseconds */
    private static final Integer PLOG_INTERVAL_TIME_MS_DEFAULT = 500;
    /** Describe PLOG wait time interval property string */
    private static final String PLOG_INTERVAL_TIME_MS_DOC =
        "Time in milliseconds of one wait interval, used by scans and " +
        "health check";
    /** Display name for scan wait time configuration parameter */
    private static final String PLOG_INTERVAL_TIME_MS_DISPLAY =
        "Interval/Wait time ms";

    /** PLOG scan interval count configuration property */
    public static final String PLOG_SCAN_INTERVAL_COUNT_CONFIG =
        "plog.scan.interval.count";
    /**  PLOG scan interval count default */
    private static final Integer PLOG_SCAN_INTERVAL_COUNT_DEFAULT = 5;
    /** Describe PLOG scan interval count property */
    private static final String PLOG_SCAN_INTERVAL_COUNT_DOC =
        "The number of intervals to wait between scans";
    /** Display name for scan wait interval count configuration parameter */
    private static final String PLOG_SCAN_INTERVAL_COUNT_DISPLAY =
        "Scan Interval Count";
 
    /** PLOG health check count configuration property */
    public static final String PLOG_HEALTH_CHECK_INTERVAL_CONFIG =
        "plog.health.check.interval";
    /** PLOG health check count defaul value */
    private static final Integer PLOG_HEALTH_CHECK_INTERVAL_DEFAULT = 10;
    /** Describe PLOG health check count variable */
    private static final String PLOG_HEALTH_CHECK_INTERVAL_DOC =
        "The number of intervals between health checks";
    /** Display name for health check scan interval count configuration
     *  parameter */
    private static final String PLOG_HEALTH_CHECK_INTERVAL_DISPLAY =
        "Health Check Interval Count";
 
    /** PLOG scan offline interval count configuration for a time out */
    public static final String PLOG_SCAN_OFFLINE_INTERVAL_CONFIG =
        "plog.scan.offline.interval";
    /** PLOG scan offline interval count default */
    private static final Integer PLOG_SCAN_OFFLINE_INTERVAL_DEFAULT = 100;
    /** Describe PLOG scan offline interval configuration property */
    private static final String PLOG_SCAN_OFFLINE_INTERVAL_DOC =
        "The number of health check scans to use for deciding whether or " +
        "not replicate is offline";
    /** Display name for offline scan interval count configuration parameter */
    private static final String PLOG_SCAN_OFFLINE_INTERVAL_DISPLAY =
        "Scan Offline Check Interval";
    
    /** Configuration parameter for the format of CDC data published */
    public static final String CONNECTOR_PUBLISH_CDC_FORMAT_CONFIG =
        "connector.publish.cdc.format";
    /** Default for publishing CDC format is change row */
    private static final String CONNECTOR_PUBLISH_CDC_FORMAT_DEFAULT = 
        ConnectorCDCFormat.CHANGEROW.toString();
    /** Documentation for supported CDC format of messages */
    private static final String CONNECTOR_PUBLISH_CDC_FORMAT_DOC =
        "Specify the CDC format to be published by connector, it determines " +
        "the type of messages published to topics, eg. changerow - is the "   +
        "full change row record after the specified change was applied and "  +
        "changeset - is the change vector only, as in the KEY, NEW and OLD "  +
        "fields for the change, note: KEY is not always PK, but what has "    +
        "been supplementally logged by system";
    /** Display name for CDC format configuration parameter */
    private static final String CONNECTOR_PUBLISH_CDC_FORMAT_DISPLAY =
        "Replicate Connector CDC Publish Format";

    /** Configuration parameter to determine whether or not the transaction
     *  information should be published to topic */
    public static final String CONNECTOR_PUBLISH_TX_INFO_CONFIG =
        "connector.publish.transaction.info";
    /** Default is to publish transaction meta data */
    private static final boolean CONNECTOR_PUBLISH_TX_INFO_DEFAULT = true;
    /** Documentation for supported CDC format of messages */
    private static final String CONNECTOR_PUBLISH_TX_INFO_DOC =
        "Whether or not the Replicate Connector should publish transaction " +
        "info meta data to topics, if true transaction meta data will be "   +
        "published to a topic, as specified in topic.name.transaction.info " +
        "and each topic message will contain extra fields to link it to "    +
        "this topic, when false, none will be published";
    /** Display name for CDC format configuration parameter */
    private static final String CONNECTOR_PUBLISH_TX_INFO_DISPLAY =
        "Publish transaction meta data to topics";

    /** Configuration parameter to determine whether or not the record keys 
     *  should be used published and used to partition topics */
    public static final String CONNECTOR_PUBLISH_KEYS_CONFIG =
        "connector.publish.keys";
    /** Default is to not publish message keys */
    private static final boolean CONNECTOR_PUBLISH_KEYS_DEFAULT = 
        false;
    /** Documentation for supported CDC format of messages */
    private static final String CONNECTOR_PUBLISH_KEYS_DOC =
        "Whether or not the Replicate Connector should publish keys to " +
        "topic messages, these are the keys that are supplementally "    +
        "logged by source system, may be primary key, unique key or "    +
        "lacking these, all columns present.If true publish message "    +
        "keys, when false none is published (the default option)";
    /** Display name for CDC format configuration parameter */
    private static final String CONNECTOR_PUBLISH_KEYS_DISPLAY =
        "Publish message keys to topics";

    /** Configuration parameter to disable schema evolution */
    public static final String CONNECTOR_PUBLISH_NO_SCHEMA_EVOLUTION_CONFIG =
        "connector.publish.no.schema.evolution";
    /** Default, for now, is to disable schema evolution, that is no existing
     *  columns can be modified or new ones added by DDL, only old ones can
     *  be removed as they are converted to optional fields */
    private static final Boolean CONNECTOR_PUBLISH_NO_SCHEMA_EVOLUTION_DEFAULT =
        true;
    /** Documentation for disabling schema evolution */
    private static final String CONNECTOR_PUBLISH_NO_SCHEMA_EVOLUTION_DOC =
        "Whether or not schema evolution should be disabled for DDL changes " +
        "that modified existing or added new fields to schema definition. "   +
        "This is only needed if integrated with a schema registry and an "    +
        "issue is preventing publishing messages";
    /** Display name for disabling schema evolution */
    private static final String CONNECTOR_PUBLISH_NO_SCHEMA_EVOLUTION_DISPLAY =
        "Disable schema evolution, eg. disabling adding new fields or " +
        "making incompatible field changes";
    
    /** Configuration to specify which broker the connector should use
     *  for persisting its internal catalog */
    public static final String CONNECTOR_CATALOG_BOOTSTRAP_SERVERS_CONFIG =
        "connector.catalog.bootstrap.servers";
    /** Default broker for connector is default local host broker */
    private static final String CONNECTOR_CATALOG_BOOTSTRAP_SERVERS_DEFAULT =
        "localhost:9092";
    /** The documentation for configuration of catalog topic brokers */
    private static final String CONNECTOR_CATALOG_BOOTSTRAP_SERVERS_DOC =
        "Provide the broker(s) to use by connector for storing its " +
        "internal catalog required to coordinate task configuration";
    /** Display name for configuration of catalog topic brokers */
    private static final String CONNECTOR_CATALOG_BOOTSTRAP_SERVERS_DISPLAY =
        "The kafka broker(s) to use by connector for storing its internal " +
        "catalog topic";
    
    /** Configure the topic name to use for internal catalog */
    public static final String CONNECTOR_CATALOG_TOPIC_NAME_CONFIG =
        "connector.catalog.topic.name";
    /** By default the topic name is the same as group name */
    private static final String CONNECTOR_CATALOG_TOPIC_NAME_DEFAULT =
        ReplicateInfoTopic.REPLICATE_INFO_GROUP;
    /** The documentation for internal catalog topic name */
    private static final String CONNECTOR_CATALOG_TOPIC_NAME_DOC =
        "Provide the name for topic to use by connector for storing its " +
        "internal catalog required to coordinate task configuration";
    /** Display name for internal catalog topic name */
    private static final String CONNECTOR_CATALOG_TOPIC_NAME_DISPLAY =
        "The topic name for sorting the connector internal catalog records";
    
    /** Topic prefix configuration property */ 
    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    /** Description of prefix configuration property */
    private static final String TOPIC_PREFIX_DOC =
        "Prefix to prepend to table names to generate the name of the " +
        "Kafka topic to publish data to";
    /** Display name for stopic prefix configuration parameter */
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
    private static final String TOPIC_NAME_TRANSACTION_INFO_DEFAULT = "TX.META";
    /** Display name for transaction info topic configuration parameter */
    private static final String TOPIC_NAME_TRANSACTION_INFO_DISPLAY =
        "Transaction Info Topic Name";
    
    /** List of schemas that can considered static in Kafka */
    public static final String TOPIC_STATIC_SCHEMAS = "topic.static.schemas";
    /** Documentation for static schema configuration option */
    private static final String TOPIC_STATIC_SCHEMAS_DOC =
        "Define the source schemas, as comma separated list of fully "        +
        "qualified replicated table names, eg. SCHEMA.TABLE, that should be " +
        "considered static or only receiving sporadic updates. The "          +
        "committed offsets of their last message can be safely ignored if "   +
        "the lapsed days between the source PLOG of a new message and that "  +
        "of a previous one is more than 'topic.static.offsets.age.days'";
    /** Display string for static schema configuration option */
    private static final String TOPIC_STATIC_SCHEMAS_DISPLAY = 
        "List of source schemas (fully qualified replicated table names) "   + 
        "that rarely or never changes. These can be considered static when " +
        "continuing publishing and the gap since last update exceeds "       +
        "'topic.static.offsets.age.days'";
    
    /** The age of when committed offsets for static tables can be safely
     *  ignored during a Kafka task restart or reconfiguration
     */
    public static final String TOPIC_STATIC_OFFSETS_AGE_DAYS = 
        "topic.static.offsets.age.days";
    /** The default number of days when to ignore stored offsets for static
     *  schemas
     */
    private static final int TOPIC_STATIC_OFFSETS_AGE_DAYS_DEFAULT = 7;
    /** Documentation for the static offset age (days) configuration option */
    private static final String TOPIC_STATIC_OFFSETS_AGE_DAYS_DOC =
        "The age of the last committed offset for a static schema "         + 
        "('topic.static.schemas'), when it can be safely ignored during "   + 
        "a task restart and stream rewind. A message that originated "      +
        "from a source PLOG older will be considered static and not "       +
        "restart at its original source PLOG stream offset, but instead "   +
        "at its next available message offset. This is intended for "       +
        "static look up tables that rarely change when their source PLOGs " +
        "may have been flushed since their last update";
    /** Display string for the static offset age (days) configuration option */
    private static final String TOPIC_STATIC_OFFSETS_AGE_DAYS_DISPLAY =
        "The age (in days) of when last committed offset for a schema " +
        "may be considered as static and can be safel ignored during "  +
        "a task restart";
    
    /** User specified global SCN to start processing or loading records from */
    public static final String GLOBAL_SCN_COLD_START_CONFIG =
        "plog.global.scn.cold.start";
    /** Description for SCN filter configuration property */
    private static final String GLOBAL_SCN_COLD_START_DOC = 
        "Global SCN to use start prcessing or loading records from, for a " +
        "cold start, eg. no data published to topic yet";
    /** Use null as default for SCN filter */
    private static final Long GLOBAL_SCN_COLD_START_DEFAULT = null;
    /** Display name for global SCN filter configuration parameter */
    private static final String GLOBAL_SCN_COLD_START_DISPLAY =
        "Global SCN Start Filter";
    
    /** Group of replicate connector configuration */
    private static final String CONNECTOR_GROUP = "Connector";
    /** Group of connector topic configuration */
    private static final String TOPIC_GROUP = "Topic";
    /** Group of data filter configuration */
    private static final String FILTER_GROUP = "Filter";
    
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
                PLOG_LOCATION_DOC,
                CONNECTOR_GROUP,
                1,
                Width.LONG,
                PLOG_LOCATION_DISPLAY
            )
            .define (
                PLOG_DATA_FLUSH_CONFIG, 
                Type.INT,
                PLOG_DATA_FLUSH_DEFAULT,
                Importance.LOW, 
                PLOG_DATA_FLUSH_DOC,
                CONNECTOR_GROUP,
                2,
                Width.SHORT,
                PLOG_DATA_FLUSH_DISPLAY
            )
            .define (
                PLOG_INTERVAL_TIME_MS_CONFIG, 
                Type.INT,
                PLOG_INTERVAL_TIME_MS_DEFAULT,
                Importance.LOW,
                PLOG_INTERVAL_TIME_MS_DOC,
                CONNECTOR_GROUP,
                3,
                Width.SHORT,
                PLOG_INTERVAL_TIME_MS_DISPLAY
            )
            .define (
                PLOG_SCAN_INTERVAL_COUNT_CONFIG, 
                Type.INT,
                PLOG_SCAN_INTERVAL_COUNT_DEFAULT,
                Importance.LOW, 
                PLOG_SCAN_INTERVAL_COUNT_DOC,
                CONNECTOR_GROUP,
                4,
                Width.SHORT,
                PLOG_SCAN_INTERVAL_COUNT_DISPLAY
            )
            .define (
                PLOG_HEALTH_CHECK_INTERVAL_CONFIG, 
                Type.INT,
                PLOG_HEALTH_CHECK_INTERVAL_DEFAULT,
                Importance.LOW, 
                PLOG_HEALTH_CHECK_INTERVAL_DOC,
                CONNECTOR_GROUP,
                5,
                Width.SHORT,
                PLOG_HEALTH_CHECK_INTERVAL_DISPLAY
            )
            .define (
                PLOG_SCAN_OFFLINE_INTERVAL_CONFIG, 
                Type.INT,
                PLOG_SCAN_OFFLINE_INTERVAL_DEFAULT,
                Importance.LOW, 
                PLOG_SCAN_OFFLINE_INTERVAL_DOC,
                CONNECTOR_GROUP,
                6,
                Width.SHORT,
                PLOG_SCAN_OFFLINE_INTERVAL_DISPLAY
            )
            .define (
                CONNECTOR_PUBLISH_CDC_FORMAT_CONFIG, 
                Type.STRING,
                CONNECTOR_PUBLISH_CDC_FORMAT_DEFAULT,
                Importance.LOW, 
                CONNECTOR_PUBLISH_CDC_FORMAT_DOC,
                CONNECTOR_GROUP,
                7,
                Width.SHORT,
                CONNECTOR_PUBLISH_CDC_FORMAT_DISPLAY
             )
            .define (
                CONNECTOR_PUBLISH_TX_INFO_CONFIG, 
                Type.BOOLEAN,
                CONNECTOR_PUBLISH_TX_INFO_DEFAULT,
                Importance.LOW, 
                CONNECTOR_PUBLISH_TX_INFO_DOC,
                CONNECTOR_GROUP,
                8,
                Width.SHORT,
                CONNECTOR_PUBLISH_TX_INFO_DISPLAY
            )
            .define (
                CONNECTOR_PUBLISH_KEYS_CONFIG, 
                Type.BOOLEAN,
                CONNECTOR_PUBLISH_KEYS_DEFAULT,
                Importance.LOW, 
                CONNECTOR_PUBLISH_KEYS_DOC,
                CONNECTOR_GROUP,
                9,
                Width.SHORT,
                CONNECTOR_PUBLISH_KEYS_DISPLAY
            )
            .define (
                CONNECTOR_PUBLISH_NO_SCHEMA_EVOLUTION_CONFIG, 
                Type.BOOLEAN,
                CONNECTOR_PUBLISH_NO_SCHEMA_EVOLUTION_DEFAULT,
                Importance.LOW, 
                CONNECTOR_PUBLISH_NO_SCHEMA_EVOLUTION_DOC,
                CONNECTOR_GROUP,
                10,
                Width.SHORT,
                CONNECTOR_PUBLISH_NO_SCHEMA_EVOLUTION_DISPLAY
            )
            .define (
                CONNECTOR_CATALOG_BOOTSTRAP_SERVERS_CONFIG,
                Type.STRING,
                CONNECTOR_CATALOG_BOOTSTRAP_SERVERS_DEFAULT,
                Importance.HIGH,
                CONNECTOR_CATALOG_BOOTSTRAP_SERVERS_DOC,
                CONNECTOR_GROUP,
                11,
                Width.LONG,
                CONNECTOR_CATALOG_BOOTSTRAP_SERVERS_DISPLAY
            )
            .define (
                CONNECTOR_CATALOG_TOPIC_NAME_CONFIG,
                Type.STRING,
                CONNECTOR_CATALOG_TOPIC_NAME_DEFAULT,
                Importance.LOW,
                CONNECTOR_CATALOG_TOPIC_NAME_DOC,
                CONNECTOR_GROUP,
                12,
                Width.LONG,
                CONNECTOR_CATALOG_TOPIC_NAME_DISPLAY
            )
            .define (
                TOPIC_PREFIX_CONFIG, 
                Type.STRING, 
                TOPIC_PREFIX_DEFAULT,
                Importance.LOW, 
                TOPIC_PREFIX_DOC,
                TOPIC_GROUP,
                1,
                Width.LONG,
                TOPIC_PREFIX_DISPLAY
            )
            .define (
                TOPIC_NAME_TRANSACTION_INFO_CONFIG,
                Type.STRING,
                TOPIC_NAME_TRANSACTION_INFO_DEFAULT,
                Importance.LOW,
                TOPIC_NAME_TRANSACTION_INFO_DOC,
                TOPIC_GROUP,
                2,
                Width.LONG,
                TOPIC_NAME_TRANSACTION_INFO_DISPLAY
            )
            .define (
                TOPIC_STATIC_SCHEMAS,
                Type.LIST,
                null,
                Importance.LOW,
                TOPIC_STATIC_SCHEMAS_DOC,
                TOPIC_GROUP,
                3,
                Width.LONG,
                TOPIC_STATIC_SCHEMAS_DISPLAY
            )
            .define (
                TOPIC_STATIC_OFFSETS_AGE_DAYS,
                Type.INT,
                TOPIC_STATIC_OFFSETS_AGE_DAYS_DEFAULT,
                Importance.LOW,
                TOPIC_STATIC_OFFSETS_AGE_DAYS_DOC,
                TOPIC_GROUP,
                4,
                Width.SHORT,
                TOPIC_STATIC_OFFSETS_AGE_DAYS_DISPLAY
            )
            .define (
                GLOBAL_SCN_COLD_START_CONFIG,
                Type.LONG,
                GLOBAL_SCN_COLD_START_DEFAULT,
                Importance.LOW,
                GLOBAL_SCN_COLD_START_DOC,
                FILTER_GROUP,
                1,
                Width.SHORT,
                GLOBAL_SCN_COLD_START_DISPLAY
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
