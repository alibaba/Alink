package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

public class FileSystemUtils {
	public static FileSystem getFlinkFileSystem(BaseFileSystem fileSystem, String path) {
		return fileSystem.load(new Path(path));
	}
}
