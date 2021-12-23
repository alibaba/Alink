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

import com.google.common.base.Preconditions;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

public class SysUtil {
	public static final Unsafe UNSAFE;
	private static String rootPath = null;
	private static String projectVersion = null;
	private static String PARENT_NAME = "flink_ai_extended";
	private static Logger LOG = LoggerFactory.getLogger(SysUtil.class);

	static {
		Unsafe instance;
		try {
			final Field field = Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			instance = (Unsafe) field.get(null);
		} catch (Exception ignored) {
			// Some platforms, notably Android, might not have a sun.misc.Unsafe
			// implementation with a private `theUnsafe` static instance. In this
			// case we can try and call the default constructor, which proves
			// sufficient for Android usage.
			try {
				Constructor<Unsafe> c = Unsafe.class.getDeclaredConstructor();
				c.setAccessible(true);
				instance = c.newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		UNSAFE = instance;
	}

	/**
	 * @return call function name.
	 */
	public static String _FUNC_() {
		StackTraceElement traceElement = ((new Exception()).getStackTrace())[1];
		return traceElement.getMethodName();
	}

	public static void sleepQuietly(int millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e1) {
		}
	}

	/**
	 * set root path by module name.
	 * @param name module name.
	 */
	public static void setParentName(String name){
		PARENT_NAME = name;
	}

	/**
	 * @return maven project root path.
	 */
	public static String getProjectRootPath() {
		if (rootPath == null) {
			// assume the working dir is under root
			File file = new File(System.getProperty("user.dir"));
			while (file != null) {
				File pom = new File(file, "pom.xml");
				if (pom.exists()) {
					try (FileReader fileReader = new FileReader(pom)) {
						MavenXpp3Reader reader = new MavenXpp3Reader();
						Model model = reader.read(fileReader);
						if (model.getArtifactId().equals(PARENT_NAME)) {
							rootPath = file.getAbsolutePath();
							break;
						}
					} catch (XmlPullParserException | IOException e) {
						LOG.error("Error reading pom files", e);
						break;
					}
				}
				file = file.getParentFile();
			}
		}
		Preconditions.checkState(rootPath != null, "Cannot determine the project's root path");
		return rootPath;
	}

	 /**
	  *  @return maven project current version.
	  */
	public static String getProjectVersion() {
		if (projectVersion == null) {
			// assume the working dir is under root
			File file = new File(System.getProperty("user.dir"));
			while (file != null) {
				File pom = new File(file, "pom.xml");
				if (pom.exists()) {
					try (FileReader fileReader = new FileReader(pom)) {
						MavenXpp3Reader reader = new MavenXpp3Reader();
						Model model = reader.read(fileReader);
						if (model.getArtifactId().equals(PARENT_NAME)) {
							projectVersion = model.getVersion();
							break;
						}
					} catch (XmlPullParserException | IOException e) {
						LOG.error("Error reading pom files", e);
						break;
					}
				}
				file = file.getParentFile();
			}
		}
		Preconditions.checkState(projectVersion != null, "Cannot determine the project's root path");
		return projectVersion;
	}
}
