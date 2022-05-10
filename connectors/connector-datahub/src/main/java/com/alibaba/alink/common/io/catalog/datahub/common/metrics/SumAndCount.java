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

package com.alibaba.alink.common.io.catalog.datahub.common.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

/**
 * SumAndCount Metric accumulates sum/count and recent avg of reported values.
 */
public class SumAndCount {
	private double sum;
	private Counter count;

	private static final long AVG_INTERVAL = 10_000L;
	private long currentAvgTime;
	private double currentAvg;
	private long nextIntervalKey;
	private double avgSum;
	private int avgCount;

	public SumAndCount(String name, MetricGroup metricGroup) {
		MetricGroup group = metricGroup.addGroup(name);
		count = group.counter("count");
		group.gauge("sum", new Gauge<Double>() {
			@Override
			public Double getValue() {
				return sum;
			}
		});
		group.gauge("avg", new Gauge<Double>() {
			@Override
			public Double getValue() {
				if (System.currentTimeMillis() - currentAvgTime > AVG_INTERVAL) {
					return 0.0;
				}
				return currentAvg;
			}
		});
	}

	/**
	 * Used only for testing purpose. Don't use in production!
	 */
	@VisibleForTesting
	public SumAndCount(String name) {
		sum = 0;
		count = new SimpleCounter();
	}

	public void update(long value) {
		update(1, value);
	}

	public void update(long countUpdated, long value) {
		count.inc(countUpdated);
		sum += value;

		long now = System.currentTimeMillis();
		if (now / AVG_INTERVAL > nextIntervalKey) {
			nextIntervalKey = now / AVG_INTERVAL;
			currentAvgTime = nextIntervalKey * AVG_INTERVAL;
			currentAvg = avgCount == 0 ? 0 : avgSum / avgCount;
			avgCount = 0;
			avgSum = 0;
		}
		avgCount++;
		avgSum += value;
	}

	public double getSum() {
		return sum;
	}

	public Counter getCounter() {
		return count;
	}
}
