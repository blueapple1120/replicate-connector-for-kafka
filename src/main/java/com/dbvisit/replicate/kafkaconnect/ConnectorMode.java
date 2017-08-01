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

/**
 * Define mode of operation for Replicate Connector
 */
public class ConnectorMode {
    /** The CDC format and parser to use for replicate connector */
    final private ConnectorCDCFormat cdcFormat;
    /** Whether or not to publish transaction info topic messages */
    final private boolean publishTxInfo;
    /** Whether or not to publish keys for topic messages */
    final private boolean publishKeys;
    /** whether or not to disable evolution of schema */
    final private boolean noSchemaEvolution; 
    
    /**
     * Create mode of operation for connector
     * 
     * @param cdcFormat         change data format to parse and publish
     * @param publishTxInfo     whether or not to publish transaction info messages
     * @param publishKeys       whether or not to publish message keys
     * @param noSchemaEvolution whether or not to disable changes to schema
     */
    public ConnectorMode (
        ConnectorCDCFormat cdcFormat,
        boolean publishTxInfo,
        boolean publishKeys,
        boolean noSchemaEvolution
    ) {
        this.cdcFormat         = cdcFormat;
        this.publishTxInfo     = publishTxInfo;
        this.publishKeys       = publishKeys;
        this.noSchemaEvolution = noSchemaEvolution;
    }
    
    /**
     * Return the CDC format for this mode of operation of the connector
     * 
     * @return CDC format, eg. change row or change set
     */
    public ConnectorCDCFormat getCDCFormat () {
        return this.cdcFormat;
    }
    
    /**
     * Return whether or not transaction info topic will be present and 
     * all topic messages will link to this topic
     * 
     * @return true if transaction info messages should be published else false
     */
    public boolean publishTxInfo () {
        return this.publishTxInfo;
    }
    
    /**
     * Return whether or not keys should be published for all topic messages,
     * these are the keys logged by source system
     * 
     * @return true if keys should be published else false
     */
    public boolean publishKeys () {
        return this.publishKeys;
    }
    
    /** Return whether or not to disable non-backwards compatible changes to 
     *  schema for all topics, this effectively retains version 1 of the 
     *  schema except when mandatory columns are removed then version 1 
     *  cannot be retained
     * 
     * @return true of schema changes should not be published
     */
    public boolean noSchemaEvolution () {
        return this.noSchemaEvolution;
    }
}
