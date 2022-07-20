package com.alibaba.alink.common.dl.utils;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.pyrunner.TarFileUtil;
import com.alibaba.alink.common.utils.FileSystemDownloadUtils;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

public class ArchivesUtils {

	public static void decompressFile(File src, File targetDir) throws IOException {
		if (isZipFile(src)) {
			ZipFileUtil.unzipFileIntoDirectory(src, targetDir);
		} else {
			TarFileUtil.unTar(src, targetDir);
		}
	}

	/**
	 * Download and decompress an archive file to `targetDir` with all contents in the archive are just put under
	 * `targetDir`.
	 *
	 * @param path
	 * @param targetDir
	 */
	public static void downloadDecompressToDirectory(String path, File targetDir) {
		Path downloadPath = PythonFileUtils.createTempDir("download_");
		String archiveFileName = FileDownloadUtils.downloadFileToDirectory(path, downloadPath.toFile());
		File archiveFile = downloadPath.resolve(archiveFileName).toFile();
		try {
			decompressFile(archiveFile, targetDir);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException(
				String.format("Cannot decompress file %s to directory %s", archiveFile, targetDir), e);
		}
	}

	/**
	 * Download and decompress an archive file to `targetDir` with all contents in the archive are just put under
	 * `targetDir`.
	 *
	 * @param filePath
	 * @param targetDir
	 */
	public static void downloadDecompressToDirectory(FilePath filePath, File targetDir) {
		Path downloadPath = PythonFileUtils.createTempDir("download_");
		String archiveFileName;
		try {
			archiveFileName = FileSystemDownloadUtils.downloadToDirectory(filePath, downloadPath);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException(
				String.format("Failed to download file %s to directory %s",
					filePath.serialize(), downloadPath.toAbsolutePath()), e);
		}
		File archiveFile = downloadPath.resolve(archiveFileName).toFile();
		try {
			decompressFile(archiveFile, targetDir);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException(
				String.format("Cannot decompress file %s to directory %s", archiveFile, targetDir), e);
		}
	}

	/**
	 * Determine whether a file is a ZIP File.
	 */
	public static boolean isZipFile(File file) throws IOException {
		if (file.isDirectory()) {
			return false;
		}
		if (!file.canRead()) {
			throw new IOException("Cannot read file " + file.getAbsolutePath());
		}
		if (file.length() < 4) {
			return false;
		}
		DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
		int test = in.readInt();
		in.close();
		return test == 0x504b0304;
	}
}
