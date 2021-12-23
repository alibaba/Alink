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

package com.alibaba.alink.common.dl.utils;

import org.apache.flink.util.FileUtils;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.utils.DownloadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;

/**
 * before execute python script, prepare python script.
 */
public class PythonFileUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PythonFileUtils.class);
    public static boolean DELETE_TEMP_FILES_WHEN_EXIT = true;

    private static final List <String> COMPRESSED_FILE_SUFFIX = Arrays.asList(
        ".zip", ".tar.gz", ".tgz"
    );

    public static String createTempWorkDir(String prefix) {
        String dirname = FileUtils.getRandomFilename(prefix);
        String fullPathName;
        try {
            fullPathName = DownloadUtils.createLocalDirectory(dirname);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create local directory.");
        }
        LOG.info("The work dir is: {}", fullPathName);
        if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
            System.out.println("The work dir is: " + fullPathName);
        }
        if (DELETE_TEMP_FILES_WHEN_EXIT) {
            DownloadUtils.setSafeDeleteFileOnExit(fullPathName);
        }
        return fullPathName;
    }

    public static File createTempDir(String prefix) {
        try {
            return Files.createTempDirectory(prefix).toFile();
        } catch (IOException e) {
            throw new RuntimeException("Cannot create temporary directory:", e);
        }
    }

    public static boolean isLocalFile(String path) {
        return path.startsWith("file://");
    }

    public static boolean isCompressedFile(String path) {
        return COMPRESSED_FILE_SUFFIX.stream().anyMatch(getFileName(path)::endsWith);
    }

    /**
     * Extract the filename from given path.
     * <p>
     * For example, if path is `http://xxx/xxx/input.py?id=xxx`, this method returns `input.py`.
     *
     * @param path
     * @return
     */
    public static String getFileName(String path) {
        if (path.contains("?")) {
            path = path.substring(0, path.indexOf('?'));
        }
        if (path.contains("\\")) {
            return path.substring(path.lastIndexOf('\\') + 1);
        } else {
            return path.substring(path.lastIndexOf('/') + 1);
        }
    }

    /**
     * Extract the filename from given path. If the given path represents a compressed file, suffix is trimmed.
     * <p>
     * For example, if path is `http://xxx/model.tar.gz`, this method returns `model`.
     *
     * @param path
     * @return
     */
    public static String getCompressedFileName(String path) {
        String fn = getFileName(path);
        for (String suffix : COMPRESSED_FILE_SUFFIX) {
            if (fn.endsWith(suffix)) {
                return fn.substring(0, fn.length() - suffix.length());
            }
        }
        return fn;
    }

    public static void ensureParentDirectoriesExist(File file) {
        file.getParentFile().mkdirs();
    }

    public static void ensureDirectoryExists(File file) {
        file.mkdirs();
    }

    static String getFileChecksum(MessageDigest digest, File file) throws IOException {
        //Get file input stream for reading the file content
        FileInputStream fis = new FileInputStream(file);

        //Create byte array to read data in chunks
        byte[] byteArray = new byte[1024];
        int bytesCount = 0;

        //Read file data and update in message digest
        while ((bytesCount = fis.read(byteArray)) != -1) {
            digest.update(byteArray, 0, bytesCount);
        }
        ;

        //close the stream; We don't need it now.
        fis.close();

        //Get the hash's bytes
        byte[] bytes = digest.digest();

        //This bytes[] has bytes in decimal format;
        //Convert it to hexadecimal format
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
        }

        //return complete hash
        return sb.toString();
    }

    public static String getFileChecksumMD5(File file) {
        try {
            //Use MD5 algorithm
            MessageDigest md5Digest = MessageDigest.getInstance("MD5");
            //Get the checksum
            return getFileChecksum(md5Digest, file);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
