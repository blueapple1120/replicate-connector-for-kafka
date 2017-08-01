package com.dbvisit.replicate.kafkaconnect.util;

/**
 * Copyright 2017 Dbvisit Software Limited
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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dbvisit.replicate.kafkaconnect.ReplicateSourceConnectorConfig;
import com.dbvisit.replicate.plog.domain.ReplicateInfo;

/**
 * Utils class for storing and retrieving replicate info JSON records from 
 * kafka topic
 */
public class ReplicateInfoTopic {
    private static final Logger logger = LoggerFactory.getLogger (
        ReplicateInfoTopic.class
    );
    /** The group ID to use for replicate info topic */
    public final static String REPLICATE_INFO_GROUP = "replicate-info";
    private final static long REPLICATE_INFO_POLL_TIMEOUT = 10000L;
    
    private final String topicName;
    private final Producer<String, String> producer;
    private final Properties consumerProps;
    
    /** 
     * Encapsulates the logic for composing and building a configured 
     * <em>ReplicateInfoTopic</em> for PLOG monitor thread
     */
    public static class ReplicateInfoTopicComposer {
        private String topicName;
        private Producer<String, String> producer;
        private Properties consumerProps;
        
        public ReplicateInfoTopicComposer () {}
        
        /**
         * Configure the components needed to compose a valid replicate 
         * info topic
         * 
         * @param props Source connector configuration properties
         * @return this composer
         * @throws Exception for any configuration errors
         */
        public ReplicateInfoTopicComposer configure(
            final Map<String, String> props
        ) 
        throws Exception {
            ReplicateSourceConnectorConfig config;
            try {
                config = new ReplicateSourceConnectorConfig (props);
            } catch (Exception e) {
                throw new Exception (
                    "Failed crearing source connector configuration, reason: " +
                    e.getMessage(),
                    e
                );
            }
            
            try {
                configureTopicName(config);
                configureProducer(config);
                configureConsumer(config);
            }
            catch (Exception e) {
                throw new ConnectException (
                    "Failed configuring replicate info topic, reason: " +
                    e.getMessage(),
                    e
                );
            }
            return this;
        }
        
        /**
         * Configure the topic name for replicate info offset messages
         * 
         * @param config source connector configuration
         * @throws Exception any configuration errors
         */
        private void configureTopicName(ReplicateSourceConnectorConfig config)
        throws Exception
        {
            String topicPrefix = null;
            String prefix = config.getString(
                ReplicateSourceConnectorConfig.TOPIC_PREFIX_CONFIG
            );

            if (prefix != null) {
                topicPrefix = prefix.toUpperCase();
            }
            
            String topic = config.getString(
                ReplicateSourceConnectorConfig
                .CONNECTOR_CATALOG_TOPIC_NAME_CONFIG
            ).toUpperCase();
                
            topicName = (
                topicPrefix == null
                ? topic
                : topicPrefix + topic
            );
        }
        
        /**
         * Configure the Kafka producer to use for writing replicate info
         * offset messages
         * 
         * @param config source connector configuration
         * @throws Exception for any producer configuration errors
         */
        private void configureProducer(ReplicateSourceConnectorConfig config)
        throws Exception
        {
            Properties props = new Properties();
            props.put(
                "bootstrap.servers", 
                config.getString(
                    ReplicateSourceConnectorConfig
                    .CONNECTOR_CATALOG_BOOTSTRAP_SERVERS_CONFIG
                )
            );
            props.put("acks", "all");
            props.put("retries", 1);
            props.put(
                "key.serializer", 
                "org.apache.kafka.common.serialization.StringSerializer"
            );
            props.put(
                "value.serializer", 
                "org.apache.kafka.common.serialization.StringSerializer"
            );
            
            producer = new KafkaProducer <String, String>(props);
        }
        
        /**
         * Configure the Kafka consumer properties to use for consuming 
         * all replicate info offset messages as monitor's schema catalog
         * 
         * @param config source connector configuration
         * @throws Exception for any consumer configuration errors
         */
        private void configureConsumer(ReplicateSourceConnectorConfig config) 
        throws Exception
        {
            if (topicName == null || topicName.length() == 0) {
                throw new Exception (
                    "Unable to configure consumer without the name of " +
                    "the topic to subscribe to"
                );
            }
            
            consumerProps = new Properties();
            consumerProps.put(
                "bootstrap.servers", 
                config.getString(
                    ReplicateSourceConnectorConfig
                    .CONNECTOR_CATALOG_BOOTSTRAP_SERVERS_CONFIG
                )
            );
            /* consumer is used to read all messages in topic only when the
             * connector starts up, no need for parallel consumption or
             * coordination */
            consumerProps.put("group.id", topicName);
            consumerProps.put("enable.auto.commit", "false");
            consumerProps.put("auto.offset.reset", "earliest");
            consumerProps.put("session.timeout.ms", "10000");
            consumerProps.put("fetch.max.wait.ms", "1000");
            consumerProps.put(
               "key.deserializer", 
               "org.apache.kafka.common.serialization.StringDeserializer"
            );
            consumerProps.put(
               "value.deserializer", 
               "org.apache.kafka.common.serialization.StringDeserializer"
            );
        }
        
        /**
         * Validate the components need to compose a valid replicate info
         * topic
         * 
         * @throws Exception for any validation errors
         */
        private void validate() throws Exception {
            if (topicName == null || topicName.length() == 0) {
                throw new Exception (
                    "Unable to build replicate info topic, reason: " +
                    "topic name has not been configured"
                );
            }
            
            if (producer == null) {
                throw new Exception (
                    "Unable to build replicate info topic, reason: " +
                    "producer has not been configured"
                );
            }
            
            if (consumerProps == null || consumerProps.isEmpty()) {
                throw new Exception (
                    "Unable to build replicate info topic, reason: " +
                    "consumer properties has not been configured"
                );
            }
        }
        
        /**
         * Compose and build a replicate info topic
         * 
         * @return valid replicate info topic
         * @throws Exception for any validation errors
         */
        public ReplicateInfoTopic build() throws Exception {
            validate();
            return new ReplicateInfoTopic (
                topicName,
                producer,
                consumerProps
            );
        }
        
    }
    
    /**
     * Return the composer/builder to use for producing a valid replicate
     * info topic for PLOG monitor
     * 
     * @return configured and valid replicate info topic
     */
    public static ReplicateInfoTopicComposer composer () {
        return new ReplicateInfoTopicComposer();
    }
    
    /**
     * Compose the replicate info topic from its required components
     * 
     * @param topicName name of topic for replicate info messages
     * @param producer  the Kafka producer configured to write to above topic
     * @param consumerProps the properties to configure a Kafka consumer
     */
    public ReplicateInfoTopic (
        final String topicName,
        final Producer<String, String> producer,
        final Properties consumerProps
    ) {
        this.topicName     = topicName;
        this.producer      = producer;
        this.consumerProps = consumerProps;
    }
    
    /**
     * Read all JSON messages from the replicate info topic and creates the 
     * latest catalog for PLOG monitor to use for controlling Kafka task 
     * configuration. This is only done during source connector startup,
     * therefore it creates a consumer each time
     * 
     * @return synchronized sorted catalog of replicate info
     * @throws Exception for any consumer errors
     */
    public synchronized Map <String, ReplicateInfo> read () throws Exception {
        /* short lived consumer, no need to keep it alive seeing that 
         * it always consumes all messages again, the volume of messages
         * is low and the consumer doesn't need to be coordinated */
        Consumer<String, String> consumer = new KafkaConsumer<String, String> (
            consumerProps
        );
        consumer.subscribe(Arrays.asList(topicName));
        
        Map<String, ReplicateInfo> catalog = Collections.synchronizedMap(
            new LinkedHashMap<String, ReplicateInfo> ()
        );
        try {
            logger.debug (
                "Polling topic: " + topicName + " for existing replicate " +
                "catalog"
            );
            
            /* greedy implementation, always fetch all replicate info 
             * messages when source connector is started */
            ConsumerRecords<String, String> records = consumer.poll(
                REPLICATE_INFO_POLL_TIMEOUT
            );
            consumer.commitSync();
            
            if (records != null) {
                for (ConsumerRecord<String, String> record : records) {
                    logger.trace (
                        "Read " + topicName + " record, key: " + 
                        record.key() + " value: " + record.value()
                    );
                    String identifier  = record.key();
                    ReplicateInfo info = ReplicateInfo.fromJSONString(
                        record.value()
                    );
                    /* all message are consumed in order, always overwrite 
                     * with the latest info */
                    catalog.put (identifier, info);
                }
            }
        }
        catch (Exception e) {
            throw new Exception (
                "Failed to read replicate info records from topic: " + 
                topicName + ", reason: " + e.getMessage(),
                e
            );
        }
        finally {
            consumer.close();
        }
        
        logger.debug (
            "Read replicate catalog: " + catalog.toString() + " " +
            "from topic: " + topicName
        );
        return catalog;
    }
    
    /**
     * Write a replicate info record as JSON to the replicate info topic
     * 
     * @param info replicate info record
     * @throws Exception for any producer errors
     */
    public synchronized void write (ReplicateInfo info) throws Exception {
        try {
            String key   = info.getIdentifier();
            String value = info.toJSONString();
            /* write one replicate info to topic */
            logger.debug (
                "Writing " + topicName + " record, key: " + 
                key + " value: " + value
            );
            /* block to ensure catalog message is written */
            producer.send(
                new ProducerRecord<String, String> (
                    topicName, 
                    key,
                    value
                )
            ).get();
        }
        catch (Exception e) {
            throw new Exception (
                "Failed to write replicate info record: " + 
                 info.toJSONString() + ", reason: " + e.getMessage(),
                 e
            );
        }
    }
    
    /**
     * Close producer and consumer
     */
    public void close () {
        try {
            producer.close();
            consumerProps.clear();
        } catch (Exception e) {}
    }

}
