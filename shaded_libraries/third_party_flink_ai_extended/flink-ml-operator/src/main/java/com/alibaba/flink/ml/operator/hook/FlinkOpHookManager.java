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

package com.alibaba.flink.ml.operator.hook;

import com.google.common.base.Preconditions;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * execute a list of FlinkOpHook.
 * flink job operator has open and close function, machine learning cluster node was wrapped by flink operator.
 * when flink operator execute open function, will execute this class open function.
 * when flink operator execute close function, will execute this class close function.
 */
public class FlinkOpHookManager {
	private List<FlinkOpHook> hooks = new ArrayList<>();

	public FlinkOpHookManager(List<String> hookClassNames) throws Exception {
		for (String name : hookClassNames) {
			hooks.add(createHookByName(name));
		}
	}

	public void open() throws Exception {
		for (FlinkOpHook hook : hooks) {
			hook.open();
		}
	}

	public void close() throws Exception {
		for (FlinkOpHook hook : hooks) {
			hook.close();
		}
	}

	private static FlinkOpHook createHookByName(String className) throws Exception {
		try {
			Class cls = Class.forName(className);
			Preconditions.checkArgument(FlinkOpHook.class.isAssignableFrom(cls),
					"Invalid implementation class " + className);
			Constructor<FlinkOpHook> constructor = cls.getConstructor();
			return constructor.newInstance();
		} catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException |
				ClassNotFoundException e) {
			throw new Exception("Failed to create " + className, e);
		}
	}

}
