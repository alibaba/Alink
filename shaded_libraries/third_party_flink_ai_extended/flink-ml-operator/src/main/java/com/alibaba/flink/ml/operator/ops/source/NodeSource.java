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

package com.alibaba.flink.ml.operator.ops.source;

import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.operator.ops.inputformat.MLInputFormat;
import com.alibaba.flink.ml.cluster.role.BaseRole;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;

/**
 * flink source operator wrapper machine learning cluster node
 * @param <OUT> machine learning cluster node output object class.
 */
public class NodeSource<OUT> extends InputFormatSourceFunction<OUT> implements ResultTypeQueryable {

	private TypeInformation<OUT> outTypeInformation;

	public NodeSource(MLInputFormat format, TypeInformation<OUT> typeInfo) {
		super(format, typeInfo);
		outTypeInformation = typeInfo;
	}

	/**
	 * create machine learning cluster node or application master plan as flink source operator.
	 * @param mode machine learning execution mode: train inference other.
	 * @param role machine learning cluster role.
	 * @param config machine learning cluster configuration.
	 * @param outTI machine learning node output flink type information.
	 * @param <OUT> machine learning node output class.
	 * @return flink source wrapper machine learning cluster node.
	 */
	public static <OUT> NodeSource<OUT> createSource(ExecutionMode mode, BaseRole role, MLConfig config,
			TypeInformation<OUT> outTI) {
		MLInputFormat<OUT> tfInputFormat = new MLInputFormat<>(mode, role, config, outTI);
		return new NodeSource<>(tfInputFormat, outTI);
	}

	@Override
	public TypeInformation getProducedType() {
		return outTypeInformation;
	}
}
