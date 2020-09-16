/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import java.io.Serializable;
import org.apache.pulsar.client.internal.DefaultImplementation;

/**
 * Batcher builder.
 */
public interface BatcherBuilder extends Serializable {

    /**
     * Default batch message container.
     *
     * <p>incoming single messages:
     * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
     *
     * <p>batched into single batch message:
     * [(k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)]
     */
    BatcherBuilder DEFAULT = DefaultImplementation.newDefaultBatcherBuilder();

    /**
     * Key based batch message container.
     *
     * <p>incoming single messages:
     * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
     *
     * <p>batched into multiple batch messages:
     * [(k1, v1), (k1, v2), (k1, v3)], [(k2, v1), (k2, v2), (k2, v3)], [(k3, v1), (k3, v2), (k3, v3)]
     */
    BatcherBuilder KEY_BASED = DefaultImplementation.newKeyBasedBatcherBuilder();

    /**
     * transaction batch message container
     *
     * <p>incoming single messages:
     * (txn1, v1), (txn2, v1), (txn3, v1), (txn1, v2), (txn2, v2), (txn3, v2), (txn1, v3), (txn2, v3), (txn3, v3)
     *
     * <p>batched into multiple batch messages:
     * [(txn1, v1), (txn1, v2), (txn1, v3)], [(txn2, v1), (txn2, v2), (txn2, v3)], [(txn3, v1), (txn3, v2), (txn3, v3)]
     */
    BatcherBuilder TRANSACTION = DefaultImplementation.newTransactionBatcherBuilder();

    /**
     * Build a new batch message container.
     * @return new batch message container
     */
    BatchMessageContainer build();

}
