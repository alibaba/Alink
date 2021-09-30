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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * down load zip file from remote file system to local
 */
public class FileUtil {
	private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

	/**
	 * parse a path and get file name.
	 * @param path path address.
	 * @return file name.
	 */
	public static String parseFileName(String path) {
		return path.substring(path.lastIndexOf("/") + 1);
	}

	/**
	 * parse a path and get dir name.
	 * @param path path address.
	 * @return dir name.
	 */
	public static String parseDirName(String path) {
		return path.substring(0, path.lastIndexOf("."));
	}

	/**
	 * download zip file to local address.
	 * @param workDir local address.
	 * @param remotePath remote file address.
	 * @param unzipDirName local dir name.
	 * @throws IOException
	 */
	public static void downLoadZipToLocal(String workDir, String remotePath, String unzipDirName) throws IOException {
		String zipName = FileUtil.parseFileName(remotePath);
		String dirName = FileUtil.parseDirName(zipName);
		if (null != unzipDirName && !unzipDirName.isEmpty()) {
			dirName = unzipDirName;
		}
		File targetDir = new File(workDir + "/" + dirName);
		Path remote = new Path(remotePath);
		FileSystem fs = remote.getFileSystem(new Configuration());
		// virtual env is shared across jobs, so we can't use mlContext's temp dir here
		File tmp = new File(workDir + "/tmp");
		if (!tmp.exists()) {
			tmp.mkdir();
		}
		Runtime.getRuntime().addShutdownHook(new Thread(() -> FileUtils.deleteQuietly(tmp)));
		Path local = new Path(tmp.getPath(), zipName);
		LOG.info("local path:" + local.toString());
		LOG.info("remote path:" + remote.toString());
		fs.copyToLocalFile(remote, local);
		Preconditions.checkState(ShellExec.run(
				String.format("unzip -q -d %s %s", tmp.getPath(), local.toString()), LOG::info),
				"Failed to unzip file:" + local.toString());
		File tmpFile = new File(tmp, dirName);
		Preconditions.checkState(tmpFile.renameTo(targetDir), "Failed to rename "
				+ tmpFile.getAbsolutePath() + " to " + targetDir.getAbsolutePath());
		LOG.info("deployed remote file to " + targetDir.getAbsolutePath());
	}

	public static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete();
	}
}
