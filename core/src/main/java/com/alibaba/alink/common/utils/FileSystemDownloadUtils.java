package com.alibaba.alink.common.utils;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.List;

public class FileSystemDownloadUtils {
	private static final Logger LOG = LoggerFactory.getLogger(FileSystemDownloadUtils.class);

	public static void download(FilePath filePath, java.nio.file.Path localPath) throws IOException {
		LOG.info(String.format("Download from %s to %s.", filePath.serialize(), localPath.toFile().getAbsolutePath()));
		BaseFileSystem <?> fileSystem = filePath.getFileSystem();
		Path path = filePath.getPath();
		FileStatus fileStatus = fileSystem.getFileStatus(path);
		long length = fileStatus.getLen();
		try (FileOutputStream fos = new FileOutputStream(localPath.toFile())) {
			FileChannel fc = fos.getChannel();
			final int maxRetryTimes = 1 << 20;
			int retryCount = 0;
			while (fc.position() < length) {
				retryCount += 1;
				LOG.info("Retry {}: resumable download {} from {} to {}",
					retryCount, filePath.serialize(), fc.position(), length);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.printf("Retry %d: resumable download %s from %d to %d%n",
						retryCount, filePath.serialize(), fc.position(), length);
				}
				try (FSDataInputStream fis = fileSystem.open(filePath.getPath())) {
					fis.seek(fc.position());
					IOUtils.copy(fis, fos);
				} catch (IOException ex) {
					LOG.info(String.format("Retry %d failed: ", retryCount), ex);
				}
				if (retryCount > maxRetryTimes) {
					break;
				}
			}
		}
	}

	public static String downloadToDirectory(FilePath filePath, java.nio.file.Path dirPath) throws IOException {
		String filename = PythonFileUtils.getFileName(filePath.getPathStr());
		download(filePath, dirPath.resolve(filename));
		return filename;
	}

	public static void downloadDirectory(FilePath filePath, java.nio.file.Path localDir) throws IOException {
		BaseFileSystem <?> fileSystem = filePath.getFileSystem();
		FileStatus fileStatus = fileSystem.getFileStatus(filePath.getPath());
		Preconditions.checkArgument(fileStatus.isDir(), "filePath must a directory.");

		Files.createDirectories(localDir);

		FileStatus[] fileStatuses = fileSystem.listStatus(filePath.getPath());
		for (FileStatus status : fileStatuses) {
			if (status.isDir()) {
				Path path = status.getPath();
				String dirname = path.getName();
				downloadDirectory(new FilePath(path, fileSystem), localDir.resolve(dirname));
			} else {
				Path path = status.getPath();
				String filename = path.getName();
				java.nio.file.Path localPath = localDir.resolve(filename);
				download(new FilePath(path, fileSystem), localPath);
			}
		}

		List <Path> paths = fileSystem.listFiles(filePath.getPath());
		for (Path path : paths) {
			String filename = path.getName();
			java.nio.file.Path localPath = localDir.resolve(filename);
			download(new FilePath(path, fileSystem), localPath);
		}
	}

	public static void downloadFilesWithPrefix(FilePath filePath, String prefix, java.nio.file.Path localDir)
		throws IOException {
		BaseFileSystem <?> fileSystem = filePath.getFileSystem();
		FileStatus fileStatus = fileSystem.getFileStatus(filePath.getPath());
		Preconditions.checkArgument(fileStatus.isDir(), "filePath must a directory.");

		Files.createDirectories(localDir);
		List <Path> paths = fileSystem.listFiles(filePath.getPath());
		for (Path path : paths) {
			if (path.getName().startsWith(prefix)) {
				String filename = path.getName();
				java.nio.file.Path localPath = localDir.resolve(filename);
				download(new FilePath(path, fileSystem), localPath);
			}
		}
	}
}
