package com.alibaba.alink.common.dl.utils;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.function.BiConsumerWithException;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.DownloadUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class FileDownloadUtils {
	private static final Logger LOG = LoggerFactory.getLogger(FileDownloadUtils.class);

	private static final Map <String, BiConsumerWithException <String, File, IOException>> protocolDownloaderMap = new HashMap <>();

	static {
		protocolDownloaderMap.put("file://", FileDownloadUtils::copyLocalFileOrDirectory);
		protocolDownloaderMap.put("res://", FileDownloadUtils::copyResourceFile);
		protocolDownloaderMap.put("http://", FileDownloadUtils::downloadHttpFile);
		protocolDownloaderMap.put("https://", FileDownloadUtils::downloadHttpFile);
		protocolDownloaderMap.put("oss://", FileDownloadUtils::downloadOssFile);
		protocolDownloaderMap.put("hdfs://", FileDownloadUtils::downloadHdfsFile);
	}

	public static boolean isLocalPath(String path) {
		return path.startsWith("file://") || path.startsWith("res://");
	}

	/**
	 * A most general download method for all possible protocols, including file://, http(s)://, oss://, hdfs://, etc.
	 * <p>
	 * "res://" is supported for internal resources. For "file://" and "res://", directories are supported.
	 *
	 * @param path
	 * @param targetFile
	 */
	public static void downloadFile(String path, File targetFile) {
		PythonFileUtils.ensureParentDirectoriesExist(targetFile);
		LOG.info("Downloading {} to {}", path, targetFile.getAbsolutePath());
		Set <Entry <String, BiConsumerWithException <String, File, IOException>>> entries = protocolDownloaderMap
			.entrySet();
		for (Entry <String, BiConsumerWithException <String, File, IOException>> entry : entries) {
			String protocol = entry.getKey();
			if (!path.startsWith(protocol)) {
				continue;
			}
			BiConsumerWithException <String, File, IOException> downloader = entry.getValue();
			try {
				downloader.accept(path, targetFile);
			} catch (Exception e) {
				throw new AkUnclassifiedErrorException("Cannot download file from " + path, e);
			}
			return;
		}
		throw new AkUnsupportedOperationException("Unsupported path: " + path);
	}

	public static void downloadFile(String path, File targetDir, String filename) {
		downloadFile(path, new File(targetDir, filename));
	}

	public static String downloadFileToDirectory(String path, File targetDir) {
		String filename = PythonFileUtils.getFileName(path);
		downloadFile(path, targetDir, filename);
		return filename;
	}

	static void copyLocalFileOrDirectory(File src, File dst) throws IOException {
		try {
			Files.createSymbolicLink(dst.toPath(), src.toPath());
			return;
		} catch (IOException e) {
			LOG.info("Creating symbolic links from {} to {} failed.", src.getAbsolutePath(), dst.getAbsoluteFile());
		}
		if (src.isFile()) {
			FileUtils.copyFile(src, dst);
		} else if (src.isDirectory()) {
			try {
				File[] files = src.listFiles();
				if (null != files) {
					for (File file : files) {
						Files.createSymbolicLink(dst.toPath().resolve(file.getName()), file.toPath());
						if (Files.isSymbolicLink(dst.toPath())) {
							System.out.printf("Symbol %s\n", dst.toString());
						}
					}
				}
				System.out.printf("Creating symbolic links {} to {} success.\n", src.getAbsolutePath(), dst.getAbsoluteFile());
				return;
			} catch (IOException e) {
				LOG.info("Creating symbolic links for all files in {} to {} failed.", src.getAbsolutePath(), dst.getAbsoluteFile());
			}
			System.out.printf("Copying, Creating symbolic links {} to {} failed.\n", src.getAbsolutePath(), dst.getAbsoluteFile());
			FileUtils.copyDirectory(src, dst);
		}
	}

	static void copyLocalFileOrDirectory(String path, File dst) throws IOException {
		path = path.substring("file://".length());
		copyLocalFileOrDirectory(new File(path), dst);
	}

	static void copyResourceFile(String path, File dst) throws IOException {
		path = path.substring("res://".length());
		InputStream in = FileDownloadUtils.class.getResourceAsStream(path);
		FileUtils.copyInputStreamToFile(in, dst);
	}

	static void downloadHttpFile(String uri, File targetFile) throws IOException {
		String md5 = "";
		if (uri.contains("?md5=")) {
			md5 = uri.substring(uri.indexOf('=') + 1);
			uri = uri.substring(0, uri.indexOf('?'));
		}

		if (targetFile.exists() && (md5.isEmpty() || md5.equals(PythonFileUtils.getFileChecksumMD5(targetFile)))) {
			LOG.info("Resource already existed: {}", targetFile.getAbsoluteFile());
			System.out.println(
				Thread.currentThread().getName() + ": Resource existed: " + targetFile.getAbsoluteFile());
			return;
		}
		LOG.info("Start downloading {}", uri); ;
		DownloadUtils.resumableDownloadHttpFile(uri, targetFile);
		LOG.info("{} downloaded", uri);
	}

	/**
	 * The format is: oss://<bucket>/<path>/?host=<host>&access_key_id=
	 * <access_key_id>&access_key_secret=<access_key_secret>&security_token=<security_token>
	 *
	 * @param path
	 * @param targetFile
	 */
	static void downloadOssFile(String path, File targetFile) {
		int pos = path.indexOf('?');
		String url = path.substring(0, pos);
		url = url.substring("oss://".length());
		String bucket = url.substring(0, url.indexOf('/'));
		String ossPath = url.substring(url.indexOf('/') + 1);
		String params = path.substring(pos + 1);
		LOG.info("bucket: {}", bucket);
		LOG.info("ossPath: {}", ossPath);
		Map <String, String> kv = new HashMap <>();
		String[] kvPairs = params.split("&");
		for (String kvPair : kvPairs) {
			int p = kvPair.indexOf('=');
			kv.put(kvPair.substring(0, p), kvPair.substring(p + 1));
		}
		OssUtils.downloadFile(targetFile, kv.get("host"), bucket, ossPath,
			kv.get("access_key_id"), kv.get("access_key_secret"), kv.get("security_token"));
	}

	static void downloadHdfsFile(String uri, File targetFile) {
		org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(uri);
		try {
			FSDataInputStream fsDataInputStream = path.getFileSystem().open(path);
			FileUtils.copyInputStreamToFile(fsDataInputStream, targetFile);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException(
				String.format("Cannot copy HDFS file %s to %s", uri, targetFile.getAbsolutePath()), e);
		}
	}

	public static String downloadHttpOrOssFile(String path, String targetDir) {
		if (path.startsWith("/") || path.startsWith("http://") || path.startsWith("https://")) {
			return FileDownloadUtils.downloadFileToDirectory(path, new File(targetDir));
		} else if (path.startsWith("oss://")) {
			/**
			 * The format is:
			 * oss://<bucket>/<path>/?host=<host>&access_key_id=<access_key_id>&access_key_secret=<access_key_secret>
			 *     &security_token=<security_token>
			 */
			int pos = path.indexOf('?');
			String url = path.substring(0, pos);
			if (url.endsWith(".zip") || url.endsWith(".tar.gz")) {
				return FileDownloadUtils.downloadFileToDirectory(url, new File(targetDir));
			} else {
				if (!url.endsWith("/")) {
					url = url + "/";
				}
				url = url.substring(new String("oss://").length());
				String bucket = url.substring(0, url.indexOf('/'));
				String ossPath = url.substring(url.indexOf('/') + 1);
				String params = path.substring(pos + 1);
				LOG.info("bucket: {}", bucket);
				LOG.info("ossPath: {}", ossPath);
				Map<String, String> kv = new HashMap<>();
				String[] kvPairs = params.split("&");
				for (int i = 0; i < kvPairs.length; i++) {
					int p = kvPairs[i].indexOf('=');
					kv.put(kvPairs[i].substring(0, p), kvPairs[i].substring(p + 1));
				}
				OssUtils.downloadFilesToDirectory(targetDir, kv.get("host"), bucket, ossPath,
					kv.get("access_key_id"), kv.get("access_key_secret"), kv.get("security_token"));
				return ossPath;
			}
		} else {
			throw new UnsupportedOperationException(path);
		}
	}
}
