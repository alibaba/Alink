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

package com.alibaba.flink.ml.cluster;

import com.alibaba.flink.ml.cluster.master.HeartbeatListener;
import com.alibaba.flink.ml.cluster.master.HeartbeatMonitor;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;

public class HeartbeatMonitorTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(3);

	@Test
	public void testSimpleTimeout() throws Exception {
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		HeartbeatListener listener = mock(HeartbeatListener.class);
		HeartbeatMonitor monitor = new HeartbeatMonitor(listener);
		monitor.updateTimeout(TIMEOUT, executor);
		Thread.sleep(TIMEOUT.toMillis() * 2);
		verify(listener).notifyHeartbeatTimeout();
		executor.shutdown();
	}

	@Test
	public void testConcurrentUpdate() throws Exception {
		final int numThreads = 10;
		final ScheduledExecutorService executor = Executors.newScheduledThreadPool(numThreads * 2);
		final HeartbeatMonitor monitor = spy(new HeartbeatMonitor(mock(HeartbeatListener.class)));
		List<Future<?>> futures = new ArrayList<>(numThreads);
		for (int i = 0; i < numThreads; i++) {
			futures.add(executor.submit(() -> {
				monitor.updateTimeout(TIMEOUT, executor);
			}));
		}
		for (Future<?> future : futures) {
			future.get();
		}
		monitor.cancel();
		Thread.sleep(TIMEOUT.toMillis() * 2);
		// the monitor shouldn't have been fired
		verify(monitor, times(0)).run();
		executor.shutdown();
	}
}
