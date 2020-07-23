package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.utils.JsonConverter;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public final class FilePath implements Serializable {

	private Path path;
	private BaseFileSystem fileSystem;

	public FilePath(String path) {
		this(path, null);
	}

	public FilePath(String path, BaseFileSystem fileSystem) {
		this(new Path(path), fileSystem);
	}

	public FilePath(Path path) {
		this(path, null);
	}

	public FilePath(Path path, BaseFileSystem fileSystem) {
		this.path = path;
		this.fileSystem = fileSystem;

		init();
	}

	public Path getPath() {
		return path;
	}

	public String getPathStr() {
		return path.toString();
	}

	public BaseFileSystem<?> getFileSystem() {
		return fileSystem;
	}

	public String serialize() {
		return JsonConverter.toJson(FilePathJsonable.fromFilePath(this));
	}

	public static FilePath deserialize(String str) {
		return str.trim().startsWith("{") ? JsonConverter.fromJson(str, FilePathJsonable.class).toFilePath() : new FilePath(str);
	}

	private final static class FilePathJsonable implements Serializable {

		public String path;
		public Params params;

		// for json serialize.
		public FilePathJsonable() {
		}

		public FilePathJsonable(String path, Params params) {
			this.path = path;
			this.params = params;
		}

		public FilePath toFilePath() {
			return new FilePath(path, params == null ? null : BaseFileSystem.of(params));
		}

		public static FilePathJsonable fromFilePath(FilePath filePath) {
			return new FilePathJsonable(filePath.getPathStr().toString(), filePath.getFileSystem() == null ? null : filePath.getFileSystem().getParams());
		}
	}

	private void init() {
		Preconditions.checkNotNull(path, "Must be set path.");
		String schema = path.toUri().getScheme();

		if (schema != null && (schema.equals("http") || schema.equals("https"))) {
			return;
		}

		if (fileSystem == null) {
			schema = rewriteUri(path.toUri()).getScheme();

			List<String> allFileSystemNames = AnnotationUtils.allFileSystemNames();

			for (String fileSystemName : allFileSystemNames) {
				BaseFileSystem<?> localFileSystem;

				try {
					localFileSystem = AnnotationUtils.createFileSystem(fileSystemName, new Params());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}

				if (localFileSystem.getSchema().equals(schema)) {
					fileSystem = localFileSystem;
					break;
				}
			}

			if (fileSystem == null) {
				fileSystem = new HadoopFileSystem(path.toString());
			}
		}
	}

	private static URI rewriteUri(URI fsUri) {
		final URI uri;

		if (fsUri.getScheme() != null) {
			uri = fsUri;
		}
		else {
			// Apply the default fs scheme
			final URI defaultUri = LocalFileSystem.getLocalFsURI();
			URI rewrittenUri = null;

			try {
				rewrittenUri = new URI(defaultUri.getScheme(), null, defaultUri.getHost(),
					defaultUri.getPort(), fsUri.getPath(), null, null);
			}
			catch (URISyntaxException e) {
				// for local URIs, we make one more try to repair the path by making it absolute
				if (defaultUri.getScheme().equals("file")) {
					try {
						rewrittenUri = new URI(
							"file", null,
							new Path(new File(fsUri.getPath()).getAbsolutePath()).toUri().getPath(),
							null);
					} catch (URISyntaxException ignored) {
						// could not help it...
					}
				}
			}

			if (rewrittenUri != null) {
				uri = rewrittenUri;
			}
			else {
				throw new IllegalArgumentException("The file system URI '" + fsUri +
					"' declares no scheme and cannot be interpreted relative to the default file system URI ("
					+ defaultUri + ").");
			}
		}

		// print a helpful pointer for malformed local URIs (happens a lot to new users)
		if (uri.getScheme().equals("file") && uri.getAuthority() != null && !uri.getAuthority().isEmpty()) {
			String supposedUri = "file:///" + uri.getAuthority() + uri.getPath();

			throw new IllegalArgumentException("Found local file path with authority '" + uri.getAuthority() + "' in path '"
				+ uri.toString() + "'. Hint: Did you forget a slash? (correct path would be '" + supposedUri + "')");
		}

		return uri;
	}
}
