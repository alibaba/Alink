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

import org.junit.Assert;
import org.junit.Test;

public class ReflectUtilTest {
	public static class A {
		int c = 0;

		public A() {
		}

		public A(int c) {
			this.c = c;
		}

		public int code() {
			return c;
		}
	}

	@Test
	public void testCreateInstance() throws Exception {
		System.out.println(SysUtil._FUNC_());
		A a = ReflectUtil.createInstance(A.class.getName(), new Class[0], new Object[0]);
		Assert.assertEquals(0, a.code());
		Class[] classes = new Class[1];
		classes[0] = int.class;
		Object[] objects = new Object[1];
		objects[0] = 1;
		a = ReflectUtil.createInstance(A.class.getName(), classes, objects);
		Assert.assertEquals(1, a.code());
	}

}