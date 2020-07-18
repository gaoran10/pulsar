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
package org.apache.pulsar.broker.transaction.buffer.exceptions;

import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi;

/**
 * Exceptions are thrown when txnAction is unsupported.
 */
public class UnsupportedTxnActionException extends TransactionBufferException {

    private static final long serialVersionUID = 0L;

    public UnsupportedTxnActionException(TxnID txnId, PulsarApi.TxnAction txnAction) {
        super("Transaction `" + txnId + "` receive unsupported txnAction " + txnAction);
    }
}
