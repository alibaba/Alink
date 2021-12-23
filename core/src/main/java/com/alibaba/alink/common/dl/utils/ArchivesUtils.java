package com.alibaba.alink.common.dl.utils;

import com.alibaba.alink.common.pyrunner.TarFileUtil;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ArchivesUtils {

	public static void decompressFile(File src, File targetDir) throws IOException {
		if (isZipFile(src)) {
			ZipFileUtil.unzipFileIntoDirectory(src, targetDir);
		} else {
			TarFileUtil.unTar(src, targetDir);
		}
	}

	/**
	 * Download and decompress a archive file to `targetDir` with all contents in the archive are just put under
	 * `targetDir`.
	 *
	 * @param path
	 * @param targetDir
	 */
	public static void downloadDecompressToDirectory(String path, File targetDir) {
		File tempDir = PythonFileUtils.createTempDir(null);
		String archiveFileName = FileDownloadUtils.downloadFileToDirectory(path, tempDir);
		File archiveFile = new File(tempDir, archiveFileName);
		try {
			decompressFile(archiveFile, targetDir);
		} catch (IOException e) {
			throw new RuntimeException(
				String.format("Cannot decompress file %s to directory %s", archiveFile, targetDir));
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
