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

package com.alibaba.flink.ml.operator.ops;

import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.role.BaseRole;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * flink flatmap operator wrapper machine learning node. node has input and output.
 * @param <IN> node input object class.
 * @param <OUT> node output object class.
 */
public class MLFlatMapOp<IN, OUT> extends RichFlatMapFunction<IN, OUT> implements ResultTypeQueryable<OUT> {
	private MLMapFunction<IN, OUT> map;

	private Logger LOG = LoggerFactory.getLogger(MLFlatMapOp.class);

	public MLFlatMapOp(ExecutionMode mode, BaseRole job, MLConfig config, TypeInformation<IN> inTI,
			TypeInformation<OUT> outTI) {
		map = new MLMapFunction<>(mode, job, config, inTI, outTI);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		map.open(getRuntimeContext());
	}

	@Override
	public void close() {
		map.close();
	}

	@Override
	public void flatMap(IN value, Collector<OUT> out) throws Exception {
		map.flatMap(value, out);
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return map.getProducedType();
	}
}
