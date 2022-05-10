/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.common;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.util.StringUtils;

/**
 * MetricUtils.
 */
public class MetricUtils {
	public static final String METRICS_TPS = "tps";
	public static final String METRICS_DELAY = "delay";
	public static final String METRICS_FETCHED_DELAY = "fetched_delay";
	public static final String METRICS_NO_DATA_DELAY = "no_data_delay";

	public static final String METRICS_SOURCE_PROCESS_LATENCY = "sourceProcessLatency";
	public static final String METRICS_SOURCE_PARTITION_LATENCY = "partitionLatency";
	public static final String METRICS_SOURCE_PARTITION_COUNT = "partitionCount";
	public static final String METRICS_PARSER_TPS = "parserTps";
	public static final String METRICS_PARSER_SKIP_COUNTER = "parserSkipCount";
	public static final String METRICS_IN_BPS = "inBps";
	public static final String METRICS_BATCH_READ_COUNT = "batchReadCount";
	public static final String METRICS_TAG_CONNECTOR_TYPE = "connector_type";

	public static final String METRIC_GROUP_SINK = "sink";
	public static final String METRICS_SINK_IN_SKIP_COUNTER = "sinkSkipCount";
	private static final String METRICS_SINK_IN_TPS = "inTps";
	private static final String METRICS_SINK_OUT_TPS = "outTps";
	private static final String METRICS_SINK_OUT_BPS = "outBps";
	private static final String METRICS_SINK_OUT_Latency = "outLatency";

	public static Meter registerSinkInTps(RuntimeContext context) {
		Counter parserCounter = context.getMetricGroup().addGroup(METRIC_GROUP_SINK)
									.counter(METRICS_SINK_IN_TPS + "_counter", new SimpleCounter());
		return context.getMetricGroup().addGroup(METRIC_GROUP_SINK)
					.meter(METRICS_SINK_IN_TPS, new MeterView(parserCounter, 60));
	}

	public static Meter registerOutTps(RuntimeContext context) {
		Counter parserCounter = context.getMetricGroup().addGroup(METRIC_GROUP_SINK)
									.counter(METRICS_SINK_OUT_TPS + "_counter", new SimpleCounter());
		return context.getMetricGroup().addGroup(METRIC_GROUP_SINK).meter(METRICS_SINK_OUT_TPS, new MeterView(parserCounter, 60));
	}

	public static Meter registerOutBps(RuntimeContext context) {
		return registerOutBps(context, null);
	}

	public static Meter registerOutBps(RuntimeContext context, String connectorType) {
		Counter bpsCounter = context.getMetricGroup().addGroup(METRIC_GROUP_SINK)
									.counter(METRICS_SINK_OUT_BPS + "_counter", new SimpleCounter());
		String tag = "";
		if (!StringUtils.isNullOrWhitespaceOnly(connectorType)) {
			tag = ":" + METRICS_TAG_CONNECTOR_TYPE + "=" + connectorType;
		}
		return context.getMetricGroup().addGroup(METRIC_GROUP_SINK)
					.meter(METRICS_SINK_OUT_BPS + tag, new MeterView(bpsCounter, 60));
	}

	public static LatencyGauge registerOutLatency(RuntimeContext context) {
		return context.getMetricGroup().addGroup(METRIC_GROUP_SINK)
					.gauge(METRICS_SINK_OUT_Latency, new LatencyGauge());
	}

	public static Counter registerSinkSkipCounter(RuntimeContext context, String groupName) {
		return context.getMetricGroup().addGroup(METRIC_GROUP_SINK).addGroup(groupName)
					.counter(METRICS_SINK_IN_SKIP_COUNTER);
	}

	/**
	 * LatencyGauge.
	 */
	public static class LatencyGauge implements Gauge<Double> {
		private double value;

		public void report(long timeDelta, long batchSize) {
			if (batchSize != 0) {
				this.value = (1.0 * timeDelta) / batchSize;
			}
		}

		public void report(long value) {
			this.value = 1.0 * value;
		}

		public void report(double value) {
			this.value = value;
		}

		@Override
		public Double getValue() {
			return value;
		}
	}
}
