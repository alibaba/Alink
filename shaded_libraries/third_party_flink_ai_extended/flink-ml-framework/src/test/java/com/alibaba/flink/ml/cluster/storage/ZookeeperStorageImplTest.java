/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.ml.cluster.storage;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.DummyContext;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ZookeeperStorageImplTest {
	private static TestingServer server;
	private static Storage client;

	@Before
	public void setUp() throws Exception {
		server = new TestingServer(2181, true);
		MLContext context = DummyContext.createDummyMLContext();
		context.getProperties().put(MLConstants.JOB_VERSION, "0");
		client = StorageFactory.getStorageInstance(context.getProperties());
	}

	@After
	public void tearDown() throws Exception {
		client.close();
		server.stop();
	}

	@Test
	public void setGetValue() throws Exception {
		client.setValue("a", "aa".getBytes());
		byte[] bytes = client.getValue("a");
		Assert.assertEquals("aa", new String(bytes));
	}

	@Test
	public void listChildren() throws Exception {
		String path = "aa";
		int size = 3;
		for (int i = 0; i < size; i++) {
			client.setValue(path + "/" + i, String.valueOf(i).getBytes());
		}
		boolean flag1 = client.exists(path);
		assertEquals(true, flag1);
		List<String> res = client.listChildren(path);
		Assert.assertEquals(size, res.size());
		for (int i = 0; i < size; i++) {
			Assert.assertEquals(String.valueOf(i), res.get(i));
		}
		client.removeValue(path);
		boolean flag = client.exists(path);
		assertEquals(false, flag);

	}


}