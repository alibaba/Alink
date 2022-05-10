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

package com.alibaba.alink.common.io.catalog.datahub.common.reader;

import com.alibaba.alink.common.io.catalog.datahub.common.util.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Base class to monitor source partition change.
 */
public abstract class AbstractPartitionNumsListener implements Serializable{
	private static Logger logger = LoggerFactory.getLogger(AbstractPartitionNumsListener.class);
	private boolean partitionChanged = false;
	protected int initPartitionCount = -1;
	private transient Timer listener;
	private static ConnectionPool <Timer> timerPool = new ConnectionPool<>();

	public void initPartitionNumsListener(){
		scheduleListener();
	}

	public abstract int getPartitionsNums();

	public abstract String getReaderName();

	/**
	 * Start flusher that will listen shards change.
	 */
	private void scheduleListener() {
		synchronized (AbstractPartitionNumsListener.class) {
			if (timerPool.contains(getReaderName())) {
				listener = timerPool.get(getReaderName());
			} else {
				listener = new Timer("Partition Change " + getReaderName() + "-Listener");
				listener.schedule(new TimerTask() {
					@Override
					public void run() {
						try {
							// using random to reduce pressure to server
							Thread.sleep(new Random().nextInt(300) * 1000);
							int partitionsNums = getPartitionsNums();
							partitionNumsChangeListener(partitionsNums, initPartitionCount);
						} catch (Throwable e) {
							logger.error("Get partition of " + getReaderName() + " Error", e);
						}
					}
				}, 60000, 5 * 60000);
				timerPool.put(getReaderName(), listener);
			}
		}
	}

	protected void triggerPartitionNumFailOver() {
		partitionChanged = true;
		logger.error(String.format("shards number of the logStore[%s] has changed, pls adjust" +
								" source parallelism configuration and then do restart.", getReaderName()
		));
	}

	protected void partitionNumsChangeListener(int newPartitionsCount, int initPartitionCount) {
		if (newPartitionsCount != initPartitionCount) {
			triggerPartitionNumFailOver();
		}
	}

	public AbstractPartitionNumsListener setInitPartitionCount(int initPartitionCount) {
		this.initPartitionCount = initPartitionCount;
		return this;
	}

	public int getInitPartitionCount() {
		return initPartitionCount;
	}

	public boolean isPartitionChanged() {
		return partitionChanged;
	}

	public void destroyPartitionNumsListener() {
		synchronized (AbstractPartitionNumsListener.class) {
			if (timerPool.remove(getReaderName()) && null != listener) {
				listener.cancel();
				listener = null;
			}
		}
	}

}
