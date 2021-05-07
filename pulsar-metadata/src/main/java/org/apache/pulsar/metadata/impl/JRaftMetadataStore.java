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
package org.apache.pulsar.metadata.impl;

import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.configured.MultiRegionRouteTableOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;

@Slf4j
public class JRaftMetadataStore extends AbstractMetadataStore implements MetadataStoreExtended {

    private final RheaKVStore rheaKVStore = new DefaultRheaKVStore();
    private ObjectMapper objectMapper = new ObjectMapper();

    public JRaftMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig) {
        init(metadataURL);
    }


    public void init(String metadataUrl) {
        String serverList = metadataUrl.replace("jraft://", "");
        final List<RegionRouteTableOptions> regionRouteTableOptionsList = MultiRegionRouteTableOptionsConfigured
            .newConfigured() //
            .withInitialServerList(-1L /* default id */,
                "127.0.0.1:8181,127.0.0.1:8182,127.0.0.1:8183") //
            .config();
        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured() //
            .withFake(true) //
            .withRegionRouteTableOptionsList(regionRouteTableOptionsList) //
            .config();
        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured() //
            .withClusterName("rhea_example") //
            .withPlacementDriverOptions(pdOpts) //
            .config();
        System.out.println(opts);
        rheaKVStore.init(opts);
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        CompletableFuture<Optional<GetResult>> getFuture = new CompletableFuture<>();
        rheaKVStore.get(path).whenComplete((data, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get path: {}", path, throwable);
                getFuture.completeExceptionally(throwable);
                return;
            }
            if (data == null) {
                getFuture.complete(Optional.empty());
                return;
            }
            GetResult getResult = new GetResult(data, new Stat(path, -1, 0, 0, false, false));
            getFuture.complete(Optional.of(getResult));
        });
        return getFuture;
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        return put(path, value, Optional.empty(), EnumSet.noneOf(CreateOption.class));
    }

    @Override
    protected CompletableFuture<List<String>> getChildrenFromStore(String path) {
        List<String> list = new ArrayList<>();
        final RheaIterator<KVEntry> it = rheaKVStore.iterator(path, null, 100);
        while (it.hasNext()) {
            final KVEntry kv = it.next();
            String key = readUtf8(kv.getKey());
            if (key.startsWith(path)) {
                String replaceResult = key.replaceFirst(path, "");
                String[] strings = replaceResult.split("/");
                if (strings.length == 2) {
                    list.add(strings[1]);
                }
            }
        }
        return CompletableFuture.completedFuture(list);
    }

    @Override
    protected CompletableFuture<Boolean> existsFromStore(String path) {
        CompletableFuture<Boolean> existsFuture = new CompletableFuture<>();
        rheaKVStore.get(path).whenComplete((data, throwable) -> {
            if (throwable != null) {
                existsFuture.completeExceptionally(throwable);
                return;
            }
            existsFuture.complete(data != null);
            System.out.println("existsFromStore");
        });
        return existsFuture;
    }

    @Override
    protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion) {
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
        rheaKVStore.delete(path).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                log.error("Failed to delete for path: {}", path);
                deleteFuture.completeExceptionally(throwable);
                return;
            }
            deleteFuture.complete(null);
        });
        return deleteFuture;
    }

    @Override
    protected CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion, EnumSet<CreateOption> options) {
        CompletableFuture<Stat> putFuture = new CompletableFuture<>();
        rheaKVStore.put(path, data).whenComplete((flag, throwable) -> {
            if (throwable != null) {
                log.error("Failed to put data path: {}", path, throwable);
                putFuture.completeExceptionally(throwable);
                return;
            }
            putFuture.complete(new Stat(path, -1, 0, 0, false, false));
        });
        return putFuture;
    }

    @Override
    public void close() throws Exception {
        super.close();
        rheaKVStore.shutdown();
    }

    public static void main(String[] args) throws Exception {
        JRaftMetadataStore jRaftMetadataStore = new JRaftMetadataStore("jraft://" + Configs.ALL_NODE_ADDRESSES, null);

        CompletableFuture<Stat> future =
                jRaftMetadataStore.put("/ledgers", new byte[0], Optional.empty());
        jRaftMetadataStore.waitFuture(future);
        System.out.println("Finish put operation");

        future = jRaftMetadataStore.put("/ledgers/ledger-1", "leger-1".getBytes(StandardCharsets.UTF_8), Optional.empty());
        jRaftMetadataStore.waitFuture(future);

        future = jRaftMetadataStore.put("/ledgers/ledger-2", "leger-2".getBytes(StandardCharsets.UTF_8), Optional.empty());
        jRaftMetadataStore.waitFuture(future);

        future = jRaftMetadataStore.put("/ledgers/ledger-2/data", "leger-2".getBytes(StandardCharsets.UTF_8), Optional.empty());
        jRaftMetadataStore.waitFuture(future);

        CompletableFuture<Optional<GetResult>> getFuture = jRaftMetadataStore.get("/ledgers");
        jRaftMetadataStore.waitFuture(getFuture);

        CompletableFuture<List<String>> childrenFuture =
                jRaftMetadataStore.getChildrenFromStore("/ledgers");
        jRaftMetadataStore.waitFuture(childrenFuture);
        List<String> children =  childrenFuture.get();
        System.out.println("children " + children);

        CompletableFuture<Boolean> existsFuture = jRaftMetadataStore.existsFromStore("unknown");
        jRaftMetadataStore.waitFuture(existsFuture);
        System.out.println("unknown path exists is " + existsFuture.get());

        System.out.println("Finish get operation");
        jRaftMetadataStore.close();
    }

    public void waitFuture(CompletableFuture<?> future) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        future.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
            }
            countDownLatch.countDown();
        });
        countDownLatch.await();
    }

}
