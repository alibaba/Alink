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

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

/**
 * This class only set the process environment variables for current process.
 * It also changes the copy of JVM environment variables.
 *
 * Code borrowed from http://blog.quirk.es/2009/11/setting-environment-variables-in-java.html
 * And this is helpful https://stackoverflow.com/questions/580085/is-it-possible-to-set-an-environment-variable-at-runtime-from-java
 */
public class EnvVarUtil {
	public interface WinLibC extends Library {
		int _putenv(String name);
	}

	public interface LinuxLibC extends Library {
		int setenv(String name, String value, int overwrite);

		int unsetenv(String name);
	}


	private static Logger LOG = LoggerFactory.getLogger(EnvVarUtil.class);
	private static Object libc;

	static {
		if (Platform.isLinux() || Platform.isMac()) {
			libc = Native.loadLibrary("c", LinuxLibC.class);
		} else if (Platform.isWindows()) {
			libc = Native.loadLibrary("msvcrt", WinLibC.class);
		}
	}

	static int setenv(String name, String value) {
		// TODO: make this thread safe
		int res;
		if (libc instanceof LinuxLibC) {
			res = ((LinuxLibC) libc).setenv(name, value, 1);
		} else {
			res = ((WinLibC) libc)._putenv(name + "=" + value);
		}
		if (res == 0) {
			try {
				setJVMEnv(name, value);
			} catch (Exception e) {
				LOG.error("Failed to set JVM env", e);
				res = 1;
				unsetenv(name);
			}
		}
		return res;
	}

	static int unsetenv(String name) {
		// TODO: make this thread safe
		if (libc instanceof LinuxLibC) {
			return ((LinuxLibC) libc).unsetenv(name);
		} else {
			return ((WinLibC) libc)._putenv(name + "=");
		}
	}

	/**
	 * Add one environment variable to current process.
	 * This code is copied and modified from https://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java.
	 */
	private static void setJVMEnv(String name, String value) throws ClassNotFoundException, NoSuchFieldException,
			IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		try {
			Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
			Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
			theEnvironmentField.setAccessible(true);
			Map env = (Map) theEnvironmentField.get(null);
			// env is a map of <Variable, Value>
			env.put(toVariable(name), toValue(value));
			Field theCaseInsensitiveEnvironmentField = processEnvironmentClass
					.getDeclaredField("theCaseInsensitiveEnvironment");
			theCaseInsensitiveEnvironmentField.setAccessible(true);
			Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
			cienv.put(name, value);
		} catch (NoSuchFieldException e) {
			LOG.warn(e.toString());
			Class[] classes = Collections.class.getDeclaredClasses();
			Map<String, String> env = System.getenv();
			for (Class cl : classes) {
				if ("java.com.alibaba.flink.ml.examples.util.Collections$UnmodifiableMap".equals(cl.getName())) {
					Field field = cl.getDeclaredField("m");
					field.setAccessible(true);
					Object obj = field.get(env);
					Map<String, String> map = (Map<String, String>) obj;
					map.put(name, value);
				}
			}
		}
	}

	private static Object toVariable(String s) throws ClassNotFoundException, NoSuchMethodException,
			InvocationTargetException, IllegalAccessException {
		Method method = Class.forName("java.lang.ProcessEnvironment$Variable")
				.getDeclaredMethod("valueOf", String.class);
		method.setAccessible(true);
		return method.invoke(null, s);
	}

	private static Object toValue(String s) throws ClassNotFoundException, NoSuchMethodException,
			InvocationTargetException, IllegalAccessException {
		Method method = Class.forName("java.lang.ProcessEnvironment$Value").getDeclaredMethod("valueOf", String.class);
		method.setAccessible(true);
		return method.invoke(null, s);
	}
}
