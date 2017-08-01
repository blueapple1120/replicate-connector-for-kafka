package com.dbvisit.replicate.kafkaconnect;
import org.apache.kafka.connect.data.Schema;

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
public class TopicSchema {
    /** Default transaction ID field for every kafka message */
    public static final String METADATA_TRANSACTION_ID_FIELD = "XID";
    /** Default transaction change type field for every kafka message */
    public static final String METADATA_CHANGE_TYPE_FIELD = "TYPE";
    /** Default transaction change ID field for every kafka message */
    public static final String METADATA_CHANGE_ID_FIELD = "CHANGE_ID";
    
    /** Schema definition for message keys, these are supplementally logged */
    final private Schema keySchema;
    /** Schema definition for the topic message schema */
    final private Schema valueSchema;
    
    /**
     * Create a topic schema from key and value schema. Key may be null, but
     * value not.
     * 
     * @param keySchema   the schema definition for the message key
     * @param valueSchema the schema definition for the message
     * 
     * @throws Exception when invalid value schema is provided
     */
    public TopicSchema (
        Schema keySchema,
        Schema valueSchema
    ) throws Exception 
    {
        if (valueSchema == null) {
            throw new Exception (
                "Only key schema is optional for a composite topic schema. " +
                "Value schema is required and cannot be NULL"
            );
        }
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }
    
    /**
     * Return the schema for the topic message key
     * 
     * @return topic message key schema
     */
    public Schema keySchema() {
        return this.keySchema;
    }
    
    /**
     * Return the schema for the topic message
     * 
     * @return topic message schema
     */
    public Schema valueSchema() {
        return this.valueSchema;
    }

}
