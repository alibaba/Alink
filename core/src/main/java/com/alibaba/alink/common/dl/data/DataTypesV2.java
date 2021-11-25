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

package com.alibaba.alink.common.dl.data;

import java.io.Serializable;

/**
 * Modified from DataTypes in flink_ai_extended to support more array types.
 */
public enum DataTypesV2 implements Serializable {
	FLOAT_16,
	FLOAT_32,
	FLOAT_64,
	COMPLEX_64,
	COMPLEX_128,
	INT_8,
	INT_16,
	INT_32,
	INT_64,
	UINT_8,
	UINT_16,
	UINT_32,
	UINT_64,
	QINT_8,
	QUINT_8,
	QINT_16,
	QUINT_16,
	QINT_32,
	BOOL,
	STRING,
	RESOURCE,
	VARIANT,
	// Tensor types
	TENSOR,
	FLOAT_TENSOR,
	DOUBLE_TENSOR,
	SHORT_TENSOR,
	INT_TENSOR,
	LONG_TENSOR,
	BOOLEAN_TENSOR,
	BYTE_TENSOR,
	UBYTE_TENSOR,
	STRING_TENSOR,
}
