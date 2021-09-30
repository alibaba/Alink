package com.alibaba.alink.common.utils;

import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DownloadUtils {

	private static final Logger LOG = LoggerFactory.getLogger(DownloadUtils.class);

	private static long getRemoteFileLength(String remoteFilePath) {
		try {
			URL url = new URL(remoteFilePath);
			HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
			httpConnection.setRequestMethod("HEAD");
			boolean acceptRanges = "bytes".equals(httpConnection.getHeaderField("Accept-Ranges"));
			long fileLength = httpConnection.getContentLengthLong();
			httpConnection.disconnect();
			return acceptRanges ? fileLength : -1;
		} catch (Exception ex) {
			throw new RuntimeException("Cannot check range download support: " + remoteFilePath, ex);
		}
	}

	public static String resumableDownloadHttpFile(String remoteFilePath, File targetFile) {
		String targetFileName = targetFile.getName();
		String dir = targetFile.getParentFile().getAbsolutePath();
		return resumableDownloadHttpFile(remoteFilePath, dir, targetFileName);
	}

	public static String resumableDownloadHttpFile(String remoteFilePath, String dir, String targetFileName) {
		long fileLength = getRemoteFileLength(remoteFilePath);
		if (fileLength == -1) {
			return downloadHttpFile(remoteFilePath, dir, targetFileName);
		}

		Path localPath = Paths.get(dir, targetFileName).toAbsolutePath();
		File outputFile = localPath.toFile();
		URL url;
		try {
			url = new URL(remoteFilePath);
		} catch (MalformedURLException e) {
			throw new RuntimeException(String.format("%s is not a valid URL", remoteFilePath), e);
		}

		final int buffSize = 64 * 1024;    // 64KB
		byte[] buffer = new byte[buffSize];

		int maxRetryTimes = 1 << 20;
		int retryCount = 0;
		long existingFileSize = outputFile.length();
		while (existingFileSize < fileLength) {
			retryCount += 1;
			try {
				HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
				httpConnection.setRequestProperty(
					"Range",
					String.format("bytes=%d-%d", existingFileSize, fileLength - 1)
				);
				httpConnection.setRequestMethod("GET");
				httpConnection.setConnectTimeout(5000);
				httpConnection.setReadTimeout(60000);
				httpConnection.connect();
				System.out.println(String
					.format("Retry %d: resumable download %s from %d to %d", retryCount,
						remoteFilePath, existingFileSize, fileLength));

				InputStream in = httpConnection.getInputStream();
				FileOutputStream fos = new FileOutputStream(outputFile, true);

				int read;
				while ((read = in.read(buffer, 0, buffSize)) != -1) {
					fos.write(buffer, 0, read);
				}
				httpConnection.disconnect();
				fos.close();
			} catch (Exception ex) {
				LOG.info(String.format("Retry %d failed: ", retryCount), ex);
			}
			existingFileSize = outputFile.length();
			if (retryCount > maxRetryTimes) {
				break;
			}
		}
		if (existingFileSize < fileLength) {
			throw new RuntimeException(String.format("Fail to resumable download file with %d tries: %s", retryCount, remoteFilePath));
		}
		return localPath.toString();
	}

	private static String downloadHttpFile(String filePath, String dir, String targetFileName) {
		try {
			HttpURLConnection connection;
			URL url = new URL(filePath);
			connection = (HttpURLConnection) url.openConnection();
			connection.setDoInput(true);
			connection.setConnectTimeout(5000);
			connection.setReadTimeout(60000);
			connection.setRequestMethod("GET");
			connection.connect();

			String fn = dir + File.separator + targetFileName;
			//            File file = new File(fn);
			//            file.deleteOnExit();

			int read;
			final int buffSize = 64 * 1024;
			byte[] buffer = new byte[buffSize];
			InputStream in = connection.getInputStream();

			FileOutputStream fos = new FileOutputStream(fn);

			while ((read = in.read(buffer, 0, buffSize)) != -1) {
				fos.write(buffer, 0, read);
			}

			connection.disconnect();
			fos.close();

			return fn;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Fail to download file " + filePath);
		}
	}

	public static String createLocalDirectory(String prefix) throws IOException {
		if (StringUtils.isNullOrWhitespaceOnly(prefix)) {
			throw new RuntimeException("prefix is empty");
		}

		String userDir = System.getProperty("user.dir");
		if (StringUtils.isNullOrWhitespaceOnly(userDir)) {
			throw new RuntimeException("user.dir is empty");
		}

		File localDir = new File(userDir);
		// TODO: to delete
		if (new File(userDir, "local").isDirectory()) {
			/*
			 * NOTE:
			 * In cupid, ${user.dir} is on apsara disk, while ${user.dir}/local is pangu disk.
			 * We should write temp files to ${user.dir}/local
			 */
			localDir = new File(userDir, "local");
		} else if (new File(userDir, "target").isDirectory()) {
			localDir = new File(userDir, "target");
		}

		File tempDir = new File(localDir, prefix);
		if (tempDir.exists()) {
			throw new RuntimeException("directory already exists: " + tempDir.getName());
		}
		tempDir.deleteOnExit();
		if (!tempDir.mkdir()) {
			throw new IOException("Failed to create temp directory " + tempDir.getPath());
		}

		return tempDir.getPath();
	}

	public static void setSafeDeleteFileOnExit(final String pathName) {
		String userDir = System.getProperty("user.dir");
		if (StringUtils.isNullOrWhitespaceOnly(userDir)) {
			throw new RuntimeException("user.dir is empty");
		}

		File file = new File(pathName);
		if (!file.getAbsolutePath().startsWith(userDir)) {
			throw new RuntimeException("Trying to delete a file/dir outside of user directory.");
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					File file = new File(pathName);
					if (file.exists()) {
						if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
							System.out.println("exit deleting " + file.getAbsolutePath());
						}
						if (file.isFile()) {
							file.delete();
						} else if (file.isDirectory()) {
							FileUtils.deleteDirectory(file);
						}
					}
				} catch (Exception e) {
					LOG.info(e.toString());
				}
			}
		});
	}

	public static void safeDeleteFile(final String pathName) {
		String userDir = System.getProperty("user.dir");
		if (StringUtils.isNullOrWhitespaceOnly(userDir)) {
			throw new RuntimeException("user.dir is empty");
		}

		File file = new File(pathName);
		if (!file.getAbsolutePath().startsWith(userDir)) {
			throw new RuntimeException("Trying to delete a file/dir outside of user directory.");
		}

		try {
			if (file.exists()) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("deleting " + file.getName());
				}
				if (file.isFile()) {
					file.delete();
				} else if (file.isDirectory()) {
					FileUtils.deleteDirectory(file);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Fail to delete " + pathName);
		}
	}
}
