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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * Replicate Kafka source task configuration
 */
public class ReplicateSourceTaskConfig extends ReplicateSourceConnectorConfig {
    /** Property in configuration that holds the de-serialized 
     *  Replicate Schema JSON objects */
    public static final String REPLICATED_CONFIG = "replicated";
    /** Description of replicated schema property field */
    private static final String REPLICATED_DOC = 
        "Deserialized Replicate Schema JSON objects for managing replicated " +
        "schemas";

    /** Replicate source task configuration definition.
     *  <p>
     *  Cannot use Type.LIST because it defaults to using comma as
     *  object separator and for this breaks serialized ReplicateSchema 
     *  JSON, instead we use String
     *  </p>
     */
    public static ConfigDef config = baseConfigDef()
        .define(
            REPLICATED_CONFIG, 
            Type.STRING, 
            Importance.HIGH, 
            REPLICATED_DOC
        );
    
    /**
     * Construct Replicate Kafka source task configuration from property map
     * 
     * @param props Property map
     */
    public ReplicateSourceTaskConfig (Map<String, String> props) {
        super(config, props);
    }

}
