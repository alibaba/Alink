package com.alibaba.alink.common.utils;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.exceptions.AkParseErrorException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
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
			throw new AkUnclassifiedErrorException("Cannot check range download support: " + remoteFilePath, ex);
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
			throw new AkParseErrorException(String.format("%s is not a valid URL", remoteFilePath), e);
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
				LOG.info("Retry {}: resumable download {} from {} to {}",
					retryCount, remoteFilePath, existingFileSize, fileLength);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(String.format("Retry %d: resumable download %s from %d to %d",
							retryCount, remoteFilePath, existingFileSize, fileLength));
				}

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
			throw new AkUnclassifiedErrorException(
				String.format("Fail to resumable download file with %d tries: %s", retryCount, remoteFilePath));
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
			throw new AkUnclassifiedErrorException("Fail to download file " + filePath);
		}
	}
}
