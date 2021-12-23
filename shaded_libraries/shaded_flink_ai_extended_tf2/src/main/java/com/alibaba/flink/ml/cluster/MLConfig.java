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

package com.alibaba.flink.ml.cluster;

import com.alibaba.flink.ml.util.MLConstants;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * machine learning cluster configuration.
 * create machine learning cluster and MLContext(machine learning node runtime context) based on this configuration.
 */
public class MLConfig implements Serializable {
	private final String envPath;
	private String[] pythonFiles;
	private String funcName;
	private Map<String, Integer> roleParallelismMap;
	private Map<String, String> properties;

	public MLConfig(Map<String, Integer> roleParallelismMap, Map<String, String> properties, String pythonFile,
					String funName, String envPath) {
		this(roleParallelismMap, properties, StringUtils.isEmpty(pythonFile) ? null : new String[] { pythonFile },
			funName, envPath);
	}

	/**
	 * Construct a MLConfig
	 *
	 * @param roleParallelismMap the Parallelism of roles
	 * @param properties properties
	 * @param pythonFiles the python files, the first one will be the entry python file
	 * @param funName the entry function name in the first python file
	 * @param envPath virtual env package address
	 */
	public MLConfig(Map<String, Integer> roleParallelismMap, Map<String, String> properties, String[] pythonFiles,
					String funName, String envPath) {
		for (Integer i : roleParallelismMap.values()) {
			Preconditions.checkArgument(i >= 0);
		}
		this.roleParallelismMap = roleParallelismMap;
		this.properties = properties;
		this.pythonFiles = pythonFiles;
		this.funcName = funName;
		this.envPath = envPath;

		if (properties == null) {
			this.properties = new HashMap<>();
		}
		if (!this.properties.containsKey(MLConstants.JOB_VERSION)) {
			this.properties.put(MLConstants.JOB_VERSION, String.valueOf(System.currentTimeMillis()));
		}
		loadSystemConfig();
	}

	/**
	 * @return virtual env package address.
	 */
	public String getEnvPath() {
		return envPath;
	}

	/**
	 * @return machine learning python scripts.
	 */
	public String[] getPythonFiles() {
		return pythonFiles;
	}

	/**
	 * @return additional attributes.
	 */
	public Map<String, String> getProperties() {
		return properties;
	}

	/**
	 * @return machine learning script main function.
	 */
	public String getFuncName() {
		return funcName;
	}

	/**
	 * additional property setter.
	 * @param key property name.
	 * @param value property value.
	 */
	public void addProperty(String key, String value) {
		properties.put(key, value);
	}

	/**
	 * additional property getter.
	 * @param key property name.
	 * @return property value.
	 */
	public String getProperty(String key) {
		return properties.get(key);
	}

	public String getProperty(String key, String defaultVal) {
		String val = properties.get(key);
		if (val == null) {
			return defaultVal;
		}
		return val;
	}

	/**
	 * set machine learning role parallelism.
	 * @param roleName role name.
	 * @param number role parallelism.
	 */
	public void setRoleNum(String roleName, int number) {
		roleParallelismMap.put(roleName, number);
	}

	/**
	 * machine learning runner scripts setter.
	 * @param pythonFiles algorithm scripts.
	 */
	public void setPythonFiles(String[] pythonFiles) {
		this.pythonFiles = pythonFiles;
	}

	/**
	 * copy machine learning configuration.
	 * @return MLConfig object.
	 */
	public MLConfig deepCopy() {
		String[] pyFiles = Arrays.copyOf(this.pythonFiles, this.pythonFiles.length);
		HashMap<String, String> destProperties = new HashMap<>(this.properties);
		HashMap<String, Integer> roleParallelismMap = new HashMap<>(this.roleParallelismMap);
		return new MLConfig(roleParallelismMap, destProperties, pyFiles,
			String.copyValueOf(this.funcName.toCharArray()), this.envPath);
	}

	public Map<String, Integer> getRoleParallelismMap() {
		return roleParallelismMap;
	}

	public void setRoleParallelismMap(Map<String, Integer> roleParallelismMap) {
		this.roleParallelismMap = roleParallelismMap;
	}

	@Override
	public String toString() {
		return "MLConfig{" +
			"envPath='" + envPath + '\'' +
			", pythonFiles=" + Arrays.toString(pythonFiles) +
			", funcName='" + funcName + '\'' +
			", roleParallelismMap=" + roleParallelismMap.toString() +
			", properties=" + properties +
			'}';
	}

	private void loadSystemConfig() {
		System.err.println("loadSystemConfig in MLConfig not supported.");
		//Configuration configuration = new Configuration();
		//configuration.addResource(MLConstants.CONFIG_TENSORFLOW_FLINK);
		//for (Map.Entry<String, String> entry : configuration) {
		//	if (!properties.containsKey(entry.getKey())) {
		//		properties.put(entry.getKey(), entry.getValue());
		//	}
		//}
	}

	/**
	 * machine learning script main function setter.
	 * @param funcName  main function name.
	 */
	public void setFuncName(String funcName) {
		this.funcName = funcName;
	}
}
