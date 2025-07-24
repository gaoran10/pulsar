/*
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
package org.apache.pulsar.common.protocol.schema;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Schema ID.
 */
public class KeyValueSchemaId implements SchemaId {

    private final byte[] kvBytes;

    public KeyValueSchemaId(byte[] keySchemaId, byte[] valueSchemaId) {
        int keySchemaIdLength = keySchemaId == null ? 0 : keySchemaId.length;
        int valueSchemaIdLength = valueSchemaId == null ? 0 : valueSchemaId.length;
        ByteBuf buf = Unpooled.buffer(4 + keySchemaIdLength + 4 + valueSchemaIdLength);
        buf.writeInt(keySchemaIdLength);
        if (keySchemaIdLength > 0) {
            buf.writeBytes(keySchemaId);
        }
        buf.writeInt(valueSchemaIdLength);
        if (valueSchemaIdLength > 0) {
            buf.writeBytes(valueSchemaId);
        }
        kvBytes = buf.array();
    }

    @Override
    public byte[] bytes() {
        return kvBytes;
    }

}
