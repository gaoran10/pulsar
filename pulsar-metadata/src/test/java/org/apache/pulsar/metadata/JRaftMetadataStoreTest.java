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
package org.apache.pulsar.metadata;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.impl.JRaftMetadataStore;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * JRaft MetadataStore test.
 */
@Slf4j
public class JRaftMetadataStoreTest {

    private JRaftMetadataStore jRaftMetadataStore;

    @BeforeClass
    private void setup() {
        jRaftMetadataStore = new JRaftMetadataStore("127.0.0.1:8181", null);
    }

    @Test
    public void putAndGetTest() throws Exception {
        String path = "/a/b/putAndGetTest";
        byte[] data = "putAndGetTest".getBytes(StandardCharsets.UTF_8);
        jRaftMetadataStore.put(path, data, Optional.empty()).get();
        Optional<GetResult> optionalGetResult = jRaftMetadataStore.get(path).get();
        Assert.assertTrue(optionalGetResult.isPresent());
        Assert.assertEquals(data, optionalGetResult.get().getValue());
    }

    @Test
    public void deleteTest() throws Exception {
        String path = "/a/b/deleteTest";
        byte[] data = "deleteTest".getBytes(StandardCharsets.UTF_8);
        jRaftMetadataStore.put(path, data, Optional.empty()).get();

        Assert.assertTrue(jRaftMetadataStore.exists(path).get());
        jRaftMetadataStore.delete(path, Optional.empty()).get();
        Assert.assertFalse(jRaftMetadataStore.exists(path).get());
    }

    @Test
    public void getChildrenTest() throws Exception {
        String path1_1 = "/level1/a";
        String path1_2 = "/level1/b";
        String path1_3 = "/level1/c";

        String path2_1 = "/level2";
        String path2_2 = "/level2/p1/e";
        String path2_3 = "/level2/p2/e/f";
        String path2_4 = "/level2/p3/e/f";

        jRaftMetadataStore.put(path1_1, new byte[0], Optional.empty()).get();
        jRaftMetadataStore.put(path1_2, new byte[0], Optional.empty()).get();
        jRaftMetadataStore.put(path1_3, new byte[0], Optional.empty()).get();
        jRaftMetadataStore.put(path2_1, new byte[0], Optional.empty()).get();
        jRaftMetadataStore.put(path2_2, new byte[0], Optional.empty()).get();
        jRaftMetadataStore.put(path2_3, new byte[0], Optional.empty()).get();
        jRaftMetadataStore.put(path2_4, new byte[0], Optional.empty()).get();

        List<String> children = jRaftMetadataStore.getChildren("/level1").get();
        Assert.assertEquals(children.size(), 3);
        Assert.assertEquals(children.get(0), "a");
        Assert.assertEquals(children.get(1), "b");
        Assert.assertEquals(children.get(2), "c");

        children = jRaftMetadataStore.getChildren("/level2").get();
        Assert.assertEquals(children.size(), 3);
        Assert.assertEquals(children.get(0), "p1");
        Assert.assertEquals(children.get(1), "p2");
        Assert.assertEquals(children.get(2), "p3");
    }

}
