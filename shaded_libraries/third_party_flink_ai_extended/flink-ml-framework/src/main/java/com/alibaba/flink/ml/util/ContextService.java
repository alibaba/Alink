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

package com.alibaba.flink.ml.util;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.proto.ContextRequest;
import com.alibaba.flink.ml.proto.ContextResponse;
import com.alibaba.flink.ml.proto.NodeServiceGrpc;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;

/**
 * implement get machine learning node runtime context service
 */
public class ContextService extends NodeServiceGrpc.NodeServiceImplBase {

	private MLContext mlContext;

	public void setMlContext(MLContext mlContext) {
		this.mlContext = mlContext;
	}

	@Override
	public void getContext(ContextRequest request, StreamObserver<ContextResponse> responseObserver) {
		Preconditions.checkNotNull(mlContext, "mlContext not set yet.");
		ContextResponse res = ContextResponse.newBuilder().setCode(0).setContext(mlContext.toPB())
				.setMessage("").build();
		responseObserver.onNext(res);
		responseObserver.onCompleted();
	}
}
