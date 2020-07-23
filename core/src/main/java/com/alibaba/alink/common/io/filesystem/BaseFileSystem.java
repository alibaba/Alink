package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.params.io.HasIoName;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseFileSystem<T extends BaseFileSystem<T>> extends FileSystem
	implements WithParams<T>, Serializable {

	protected Params params;

	protected BaseFileSystem(Params params) {
		if (null == params) {
			this.params = new Params();
		} else {
			this.params = params.clone();
		}
		this.params.set(HasIoName.IO_NAME, AnnotationUtils.annotatedName(this.getClass()));
	}

	public static BaseFileSystem of(Params params) {
		if (BaseFileSystem.isFileSystem(params)) {
			try {
				return AnnotationUtils.createFileSystem(params.get(HasIoName.IO_NAME), params);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new RuntimeException("NOT a FileSystem parameter.");
		}
	}

	public static boolean isFileSystem(Params params) {
		if (params.contains(HasIoName.IO_NAME)) {
			return AnnotationUtils.isFileSystem(params.get(HasIoName.IO_NAME));
		} else {
			return false;
		}
	}

	@Override
	public Params getParams() {
		return params;
	}

	public FileStatus getFileStatus(String f) throws IOException {
		return getFileStatus(new Path(f));
	}

	public FSDataInputStream open(String f, int bufferSize) throws IOException {
		return open(new Path(f), bufferSize);
	}

	public FSDataInputStream open(String f) throws IOException {
		return open(new Path(f));
	}

	public FileStatus[] listStatus(String f) throws IOException {
		return listStatus(new Path(f));
	}

	public boolean exists(String f) throws IOException {
		return exists(new Path(f));
	}

	public boolean delete(String f, boolean recursive) throws IOException {
		return delete(new Path(f), recursive);
	}

	public boolean mkdirs(String f) throws IOException {
		return mkdirs(new Path(f));
	}

	@Deprecated
	public FSDataOutputStream create(
		String f,
		boolean overwrite,
		int bufferSize,
		short replication,
		long blockSize) throws IOException {

		return load(new Path(f)).create(new Path(f), overwrite, bufferSize, replication, blockSize);
	}

	@Deprecated
	public FSDataOutputStream create(String f, boolean overwrite) throws IOException {
		return load(new Path(f)).create(new Path(f), overwrite);
	}

	public FSDataOutputStream create(String f, WriteMode overwriteMode) throws IOException {
		return create(new Path(f), overwriteMode);
	}

	public boolean rename(String src, String dst) throws IOException {
		return rename(new Path(src), new Path(dst));
	}

	public boolean initOutPathLocalFS(String outPath, WriteMode writeMode, boolean createDirectory) throws IOException {
		return initOutPathLocalFS(new Path(outPath), writeMode, createDirectory);
	}

	public boolean initOutPathDistFS(String outPath, WriteMode writeMode, boolean createDirectory) throws IOException {
		return initOutPathDistFS(new Path(outPath), writeMode, createDirectory);
	}

	public List<String> listFiles(String f) throws IOException {
		return listFiles(new Path(f))
			.stream()
			.map(Path::toString)
			.collect(Collectors.toList());
	}

	public List<String> listDirectories(String f) throws IOException {
		return listDirectories(new Path(f))
			.stream()
			.map(Path::toString)
			.collect(Collectors.toList());
	}

	public List<Path> listFiles(Path f) throws IOException {
		FileStatus[] fileStatuses = listStatus(f);
		List<Path> files = new ArrayList<>();
		for (FileStatus fileStatus : fileStatuses) {
			if (!fileStatus.isDir()) {
				files.add(fileStatus.getPath());
			}
		}
		return files;
	}

	public List<Path> listDirectories(Path f) throws IOException {
		FileStatus[] fileStatuses = listStatus(f);
		List<Path> files = new ArrayList<>();
		for (FileStatus fileStatus : fileStatuses) {
			if (fileStatus.isDir()) {
				files.add(fileStatus.getPath());
			}
		}
		return files;
	}

	public abstract String getSchema();

	@Override
	public Path getWorkingDirectory() {
		return load().getWorkingDirectory();
	}

	@Override
	public Path getHomeDirectory() {
		return load().getHomeDirectory();
	}

	@Override
	public URI getUri() {
		return load().getUri();
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		return load(f).getFileStatus(f);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		return load(file.getPath()).getFileBlockLocations(file, start, len);
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		return load(f).open(f, bufferSize);
	}

	@Override
	public FSDataInputStream open(Path f) throws IOException {
		return load(f).open(f);
	}

	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		return load(f).listStatus(f);
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		return load(f).delete(f, recursive);
	}

	@Override
	public boolean mkdirs(Path f) throws IOException {
		return load(f).mkdirs(f);
	}

	@Override
	public FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
		return load(f).create(f, overwriteMode);
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		return load(src).rename(src, dst);
	}

	@Override
	public boolean isDistributedFS() {
		return load().isDistributedFS();
	}

	@Override
	public FileSystemKind getKind() {
		return load().getKind();
	}

	protected FileSystem load() {
		return load(null);
	}

	protected abstract FileSystem load(Path path);
}