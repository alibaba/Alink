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

package com.alibaba.flink.ml.cluster.master;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * handle am and node heartbeat message.
 */
public class HeartbeatMonitor implements Runnable {

	// the monitor starts with state RUNNING, once the monitor fires or is cancelled, it can't be used again
	private enum State {
		RUNNING,
		TIMEOUT,
		CANCELED
	}

	private final HeartbeatListener listener;
	private final AtomicReference<ScheduledFuture<?>> timeoutFuture = new AtomicReference<>(null);
	private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

	public HeartbeatMonitor(HeartbeatListener listener) {
		this.listener = Preconditions.checkNotNull(listener);
	}

	public void updateTimeout(Duration duration, ScheduledExecutorService executor) {
		if (state.get() == State.RUNNING) {
			cancelAndResetFuture(executor.schedule(this, duration.toMillis(), TimeUnit.MILLISECONDS));
			// in case cancel() was called, or the old task fired in between
			if (state.get() != State.RUNNING) {
				cancelAndResetFuture(null);
			}
		}
	}

	public void cancel() {
		if (state.compareAndSet(State.RUNNING, State.CANCELED)) {
			cancelAndResetFuture(null);
		}
	}

	private void cancelAndResetFuture(ScheduledFuture<?> newFuture) {
		ScheduledFuture<?> oldFuture = timeoutFuture.getAndSet(newFuture);
		if (oldFuture != null) {
			// don't interrupt because the HeartbeatListener may be performing some failover routine
			oldFuture.cancel(false);
		}
	}

	@Override
	public void run() {
		if (state.compareAndSet(State.RUNNING, State.TIMEOUT)) {
			listener.notifyHeartbeatTimeout();
		}
	}
}
