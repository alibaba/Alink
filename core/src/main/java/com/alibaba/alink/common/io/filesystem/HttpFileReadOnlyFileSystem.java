package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.annotations.FSAnnotation;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;

@FSAnnotation(name = "http/https")
public class HttpFileReadOnlyFileSystem extends BaseFileSystem <HttpFileReadOnlyFileSystem> {

	public HttpFileReadOnlyFileSystem() {
		this(new Params());
	}

	public HttpFileReadOnlyFileSystem(Params params) {
		super(params);
	}

	@Override
	public String getSchema() {
		return "http/https";
	}

	@Override
	protected FileSystem load(Path path) {
		return new HttpFileSystemImpl(path);
	}

	static class HttpFileSystemImpl extends FileSystem {

		private final Path base;

		public HttpFileSystemImpl(Path base) {
			this.base = base;
		}

		@Override
		public Path getWorkingDirectory() {
			return base;
		}

		@Override
		public Path getHomeDirectory() {
			return base;
		}

		@Override
		public URI getUri() {
			return base.toUri();
		}

		@Override
		public FileStatus getFileStatus(Path f) throws IOException {
			final Path qualified = f.makeQualified(this);

			final long fileLen = doGetLen(qualified);

			return new FileStatus() {
				@Override
				public long getLen() {
					return fileLen;
				}

				@Override
				public long getBlockSize() {
					return fileLen;
				}

				@Override
				public short getReplication() {
					return 0;
				}

				@Override
				public long getModificationTime() {
					return 0;
				}

				@Override
				public long getAccessTime() {
					return 0;
				}

				@Override
				public boolean isDir() {
					return false;
				}

				@Override
				public Path getPath() {
					return qualified;
				}
			};
		}

		@Override
		public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
			final Path path = file.getPath();

			return new BlockLocation[] {new BlockLocation() {
				@Override
				public String[] getHosts() {
					return new String[] {path.toUri().getHost()};
				}

				@Override
				public long getOffset() {
					return start;
				}

				@Override
				public long getLength() {
					return len;
				}

				@Override
				public int compareTo(BlockLocation o) {
					return Long.compare(getOffset(), o.getOffset());
				}
			}};
		}

		@Override
		public FSDataInputStream open(Path f, int bufferSize) throws IOException {
			return new HttpFileDataInputStream(f);
		}

		@Override
		public FSDataInputStream open(Path f) throws IOException {
			return open(f, 1024 * 4);
		}

		@Override
		public FileStatus[] listStatus(Path f) throws IOException {
			return new FileStatus[] {getFileStatus(f)};
		}

		@Override
		public boolean delete(Path f, boolean recursive) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean mkdirs(Path f) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean rename(Path src, Path dst) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isDistributedFS() {
			return false;
		}

		@Override
		public FileSystemKind getKind() {
			return FileSystemKind.OBJECT_STORE;
		}
	}

	static long doGetLen(Path path) {
		HttpURLConnection headerConnection = null;
		try {
			headerConnection = (HttpURLConnection) path.toUri().toURL().openConnection();
			headerConnection.setConnectTimeout(5000);
			headerConnection.setRequestMethod("HEAD");

			headerConnection.connect();
			long contentLength = headerConnection.getContentLengthLong();
			String acceptRanges = headerConnection.getHeaderField("Accept-Ranges");
			boolean splittable = acceptRanges != null && acceptRanges.equalsIgnoreCase("bytes");

			if (contentLength < 0) {
				throw new RuntimeException("The content length can't be determined.");
			}

			// If the http server does not accept ranges, then we quit the program.
			// This is because 'accept ranges' is required to achieve robustness (through re-connection),
			// and efficiency (through concurrent read).
			if (!splittable) {
				throw new RuntimeException("The http server does not support range reading.");
			}

			return contentLength;
		} catch (Exception e) {
			throw new RuntimeException("Fail to connect to http server", e);
		} finally {
			if (headerConnection != null) {
				headerConnection.disconnect();
			}
		}
	}

	static class HttpFileDataInputStream extends FSDataInputStream {
		private final Path path;
		private final long fileLen;

		private transient HttpURLConnection connection;
		private transient InputStream internal;
		private transient long pos;

		private static final int CONNECTION_TIMEOUT = 5000;
		private static final int READ_TIMEOUT = 60000;
		private static final int RETRY_TIMES = 3;

		public HttpFileDataInputStream(Path path) throws IOException {
			this.path = path;
			this.fileLen = doGetLen(path);

			createInternal(0, fileLen);
		}

		@Override
		public void seek(long desired) throws IOException {
			closeInternal();
			createInternal(desired, fileLen);
			pos = desired;
		}

		@Override
		public long getPos() throws IOException {
			return pos;
		}

		@Override
		public int read() throws IOException {

			int read = -1;

			int i = 1;

			while (i <= RETRY_TIMES) {
				try {
					read = internal.read();
					break;
				} catch (SocketTimeoutException ex) {
					if (i == RETRY_TIMES) {
						throw ex;
					}

					seek(pos);
				}
				i++;
			}

			++pos;

			return read;
		}

		private void createInternal(long start, long end) throws IOException {
			connection = (HttpURLConnection) path.toUri().toURL().openConnection();

			connection.setDoInput(true);
			connection.setConnectTimeout(CONNECTION_TIMEOUT);
			connection.setReadTimeout(READ_TIMEOUT);
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Range", String.format("bytes=%d-%d", start, end));
			connection.connect();

			internal = connection.getInputStream();

			pos = 0;
		}

		private void closeInternal() throws IOException {
			if (internal != null) {
				internal.close();
			}

			if (connection != null) {
				connection.disconnect();
			}
		}

		@Override
		public void close() throws IOException {
			super.close();

			closeInternal();
		}
	}
}
