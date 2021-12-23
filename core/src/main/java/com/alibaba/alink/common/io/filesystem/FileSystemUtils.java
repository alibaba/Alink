package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FileSystemUtils {

	public static FileSystem getFlinkFileSystem(BaseFileSystem <?> fileSystem, String path) {
		return fileSystem.load(new Path(path));
	}

	public static List <FilePath> listFilesRecursive(
		FilePath rootFolder, boolean skipFailed) throws IOException {

		BaseFileSystem <?> fileSystem = rootFolder.getFileSystem();

		List <FilePath> files = new ArrayList <>();

		FileStatus[] fileStatuses;

		try {
			fileStatuses = fileSystem.listStatus(rootFolder.getPath());
		} catch (IOException ex) {
			if (skipFailed) {
				return files;
			} else {
				throw ex;
			}
		}

		for (FileStatus fileStatus : fileStatuses) {
			if (fileStatus.isDir()) {
				files.addAll(listFilesRecursive(new FilePath(fileStatus.getPath(), fileSystem), skipFailed));
			} else {
				files.add(new FilePath(fileStatus.getPath(), fileSystem));
			}
		}

		return files;
	}

	public static List <String> listFilesRelativeRecursive(FilePath rootFolder) {
		try {
			return listFilesRelativeRecursive(rootFolder, true);
		} catch (IOException e) {
			throw new IllegalStateException();
		}
	}

	public static List <String> listFilesRelativeRecursive(
		FilePath rootFolder, boolean skipFailed) throws IOException {

		return listFilesRecursive(rootFolder, skipFailed)
			.stream()
			.map(x -> relativePath(rootFolder, x))
			.collect(Collectors.toList());
	}

	private static String relativePath(FilePath rootFolder, FilePath filePath) {
		return rootFolder
			.getPath()
			.makeQualified(rootFolder.getFileSystem())
			.toUri()
			.relativize(
				filePath
					.getPath()
					.makeQualified(filePath.getFileSystem())
					.toUri()
			)
			.getPath();
	}
}
