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
package org.apache.pulsar.broker.transaction;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.transaction.buffer.impl.PersistentTransactionBuffer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Pulsar client transaction test.
 */
@Slf4j
public class PulsarClientTransactionTest extends BrokerTestBase {

    private static final String CLUSTER_NAME = "test";
    private static final String TENANT = "tnx";
    private static final String NAMESPACE1 = TENANT + "/ns1";
    private static final String TOPIC_INPUT_1 = NAMESPACE1 + "/input1";
    private static final String TOPIC_INPUT_2 = NAMESPACE1 + "/input2";
    private static final String TOPIC_OUTPUT_1 = NAMESPACE1 + "/output1";
    private static final String TOPIC_OUTPUT_2 = NAMESPACE1 + "/output2";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        init();

        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData(brokerUrl.toString()));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createPartitionedTopic(TOPIC_INPUT_1, 1);
        admin.topics().createPartitionedTopic(TOPIC_INPUT_2, 3);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT_1, 1);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT_2, 3);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        lookupUrl = new URI(brokerUrl.toString());
        if (isTcpLookup) {
            lookupUrl = new URI(pulsar.getBrokerServiceUrl());
        }
        pulsarClient = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        Thread.sleep(1000 * 3);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void produceCommitTest() throws Exception {
        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        Transaction tnx = pulsarClientImpl.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        long txnIdMostBits = ((TransactionImpl) tnx).getTxnIdMostBits();
        long txnIdLeastBits = ((TransactionImpl) tnx).getTxnIdLeastBits();
        Assert.assertTrue(txnIdMostBits > -1);
        Assert.assertTrue(txnIdLeastBits > -1);

        PartitionedProducerImpl<byte[]> outProducer = (PartitionedProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(TOPIC_OUTPUT_1)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        int messageCnt = 5;
        String content = "Hello Txn - ";
        List<CompletableFuture<MessageId>> futureList = new ArrayList<>();
        for (int i = 0; i < messageCnt; i++) {
            CompletableFuture<MessageId> produceFuture = outProducer
                    .newMessage(tnx).value((content + i).getBytes(UTF_8)).sendAsync();
            futureList.add(produceFuture);
        }

        // TODO wait a moment for adding publish partition to transaction, need to be fixed
        Thread.sleep(1000 * 5);

        ReadOnlyCursor originTopicCursor = getOriginTopicCursor(TOPIC_OUTPUT_1, 0);
        Assert.assertNotNull(originTopicCursor);
        Assert.assertFalse(originTopicCursor.hasMoreEntries());
        originTopicCursor.close();

        futureList.forEach(messageIdFuture -> {
            try {
                messageIdFuture.get(1, TimeUnit.SECONDS);
                Assert.fail("MessageId shouldn't be get before txn commit.");
            } catch (Exception e) {
                if (e instanceof TimeoutException) {
                    log.info("This is a expected exception.");
                } else {
                    log.error("This exception is not expected.", e);
                    Assert.fail("This exception is not expected.");
                }
            }
        });

        tnx.commit().get();

        futureList.forEach(messageIdFuture -> {
            try {
                MessageId messageId = messageIdFuture.get(1, TimeUnit.SECONDS);
                Assert.assertNotNull(messageId);
                log.info("Tnx commit success! messageId: {}", messageId);
            } catch (Exception e) {
                log.error("Tnx commit failed! tnx: " + tnx, e);
                Assert.fail("Tnx commit failed! tnx: " + tnx);
            }
        });

        originTopicCursor = getOriginTopicCursor(TOPIC_OUTPUT_1, 0);
        Assert.assertNotNull(originTopicCursor);
        Assert.assertTrue(originTopicCursor.hasMoreEntries());
        List<Entry> entries = originTopicCursor.readEntries((int) originTopicCursor.getNumberOfEntries());
        Assert.assertEquals(1, entries.size());
        PulsarApi.MessageMetadata messageMetadata = Commands.parseMessageMetadata(entries.get(0).getDataBuffer());
        Assert.assertEquals(PulsarMarkers.MarkerType.TXN_COMMIT_VALUE, messageMetadata.getMarkerType());
        long commitMarkerLedgerId = entries.get(0).getLedgerId();
        long commitMarkerEntryId = entries.get(0).getEntryId();

        ReadOnlyCursor tbTopicCursor = getTBTopicCursor(TOPIC_OUTPUT_1, 0);
        Assert.assertNotNull(tbTopicCursor);
        Assert.assertTrue(tbTopicCursor.hasMoreEntries());
        long tbEntriesCnt = tbTopicCursor.getNumberOfEntries();
        log.info("transaction buffer entries count: {}", tbEntriesCnt);
        Assert.assertEquals(messageCnt + 2, tbEntriesCnt);
        entries = tbTopicCursor.readEntries((int) tbEntriesCnt);
        for (int i = 0; i < messageCnt; i++) {
            messageMetadata = Commands.parseMessageMetadata(entries.get(i).getDataBuffer());
            Assert.assertEquals(messageMetadata.getTxnidMostBits(), txnIdMostBits);
            Assert.assertEquals(messageMetadata.getTxnidLeastBits(), txnIdLeastBits);

            byte[] bytes = new byte[entries.get(i).getDataBuffer().readableBytes()];
            entries.get(i).getDataBuffer().readBytes(bytes);
            System.out.println(new String(bytes));
            Assert.assertEquals(new String(bytes), content + i);
        }

        messageMetadata = Commands.parseMessageMetadata(entries.get(messageCnt).getDataBuffer());
        Assert.assertEquals(PulsarMarkers.MarkerType.TXN_COMMITTING_VALUE, messageMetadata.getMarkerType());

        messageMetadata = Commands.parseMessageMetadata(entries.get(messageCnt + 1).getDataBuffer());
        Assert.assertEquals(PulsarMarkers.MarkerType.TXN_COMMIT_VALUE, messageMetadata.getMarkerType());
        PulsarMarkers.TxnCommitMarker commitMarker = Markers.parseCommitMarker(entries.get(messageCnt + 1).getDataBuffer());
        Assert.assertEquals(commitMarkerLedgerId, commitMarker.getMessageId().getLedgerId());
        Assert.assertEquals(commitMarkerEntryId, commitMarker.getMessageId().getEntryId());

        System.out.println("finish test");
    }

    private ReadOnlyCursor getTBTopicCursor(String topic, int partition) {
        try {
            String tbTopicName = PersistentTransactionBuffer.getTransactionBufferTopicName(
                    TopicName.get(topic).toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition);

            return pulsar.getManagedLedgerFactory().openReadOnlyCursor(
                    TopicName.get(tbTopicName).getPersistenceNamingEncoding(),
                    PositionImpl.earliest, new ManagedLedgerConfig());
        } catch (Exception e) {
            log.error("Failed to get transaction buffer topic readonly cursor.", e);
            Assert.fail("Failed to get transaction buffer topic readonly cursor.");
            return null;
        }
    }

    private ReadOnlyCursor getOriginTopicCursor(String topic, int partition) {
        try {
            String partitionTopic = TopicName.get(topic).toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
            ReadOnlyCursor readOnlyCursor = pulsar.getManagedLedgerFactory().openReadOnlyCursor(
                    TopicName.get(partitionTopic).getPersistenceNamingEncoding(),
                    PositionImpl.earliest, new ManagedLedgerConfig());
            return readOnlyCursor;
        } catch (Exception e) {
            log.error("Failed to get origin topic readonly cursor.", e);
            Assert.fail("Failed to get origin topic readonly cursor.");
            return null;
        }
    }

}
