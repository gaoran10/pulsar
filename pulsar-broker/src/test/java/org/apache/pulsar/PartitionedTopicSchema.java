package org.apache.pulsar;

import com.google.common.collect.Sets;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PartitionedTopicSchema extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        isTcpLookup = true;
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        admin.tenants().createTenant("my-property",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns");
        admin.namespaces().setNamespaceReplicationClusters("my-property/my-ns", Sets.newHashSet("test"));

        // so that clients can test short names
        admin.tenants().createTenant("public",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
        admin.topics().createPartitionedTopic(PARTITIONED_TOPIC, 3);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private final static String PARTITIONED_TOPIC = "public/default/partitioned-schema-topic";

    public void beforeProduce() throws PulsarClientException {
        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(PARTITIONED_TOPIC)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();
        consumer.close();
    }

    public void produceData() throws PulsarClientException {
        @Cleanup
        Producer<User> producer = pulsarClient.newProducer(Schema.JSON(User.class))
                .topic(PARTITIONED_TOPIC)
                .enableBatching(false)
                .roundRobinRouterBatchingPartitionSwitchFrequency(1)
                .create();

        for (int i = 0; i < 10; i++) {
            User user = new User();
            user.setName("user-" + i);
            user.setAge(18);
            user.setSex(i % 2 == 0 ? "male" : "female");
            producer.newMessage().value(user).send();
        }
        System.out.println("produce messages finish");
    }

    public void checkTopic() throws Exception {
        
    }

    public void checkPartitionedTopic() throws Exception {
        @Cleanup
        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(TopicName.get(PARTITIONED_TOPIC).getPartition(1).toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();
        Message<GenericRecord> message = consumer.receive();
        System.out.println("message " + message.getValue());
        System.out.println("receive finish.");
    }

    @Test
    public void test() throws Exception {
        beforeProduce();
        produceData();
        checkPartitionedTopic();
    }

}
