package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.TemporaryClassLoaderContext;

import com.alibaba.alink.common.io.annotations.FSAnnotation;
import com.alibaba.alink.common.io.filesystem.plugin.FileSystemClassLoaderFactory;
import com.alibaba.alink.params.io.OssFileSystemParams;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;

@FSAnnotation(name = "oss")
public final class OssFileSystem extends BaseFileSystem <OssFileSystem> {

	private static final long serialVersionUID = 5666866643106003801L;

	public final static String OSS_FILE_SYSTEM_NAME = "oss";

	private FileSystemClassLoaderFactory classLoaderFactory;
	private transient FileSystem loaded;

	public OssFileSystem(Params params) {
		super(params);
	}

	public OssFileSystem(String ossVersion, String endPoint, String bucketName, String accessId,
						 String accessKey) {
		this(ossVersion, endPoint, bucketName, accessId, accessKey, null);
	}

	public OssFileSystem(String ossVersion, String endPoint, String bucketName, String accessId,
						 String accessKey, String securityToken) {
		this(new Params());

		try {
			getParams()
				.set(OssFileSystemParams.PLUGIN_VERSION, ossVersion)
				.set(OssFileSystemParams.END_POINT, endPoint)
				.set(OssFileSystemParams.ACCESS_ID, accessId)
				.set(OssFileSystemParams.ACCESS_KEY, accessKey)
				.set(OssFileSystemParams.SECURITY_TOKEN, securityToken)
				.set(
					OssFileSystemParams.FS_URI,
					new URI(getSchema(), bucketName, null, null).toString()
				);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	protected FileSystem load(Path path) {
		if (loaded != null) {
			return loaded;
		}

		final Configuration conf = new Configuration();
		conf.setString("fs.oss.endpoint", getParams().get(OssFileSystemParams.END_POINT));

		if (getParams().get(OssFileSystemParams.ACCESS_ID) != null
			&& getParams().get(OssFileSystemParams.ACCESS_KEY) != null) {
			conf.setString("fs.oss.accessKeyId", getParams().get(OssFileSystemParams.ACCESS_ID));
			conf.setString("fs.oss.accessKeySecret", getParams().get(OssFileSystemParams.ACCESS_KEY));

			if (getParams().get(OssFileSystemParams.SECURITY_TOKEN) != null) {
				conf.setString("fs.oss.securityToken", getParams().get(OssFileSystemParams.SECURITY_TOKEN));
			}
		}

		FileSystemFactory factory = createFactory();
		factory.configure(conf);

		try {
			if (getParams().get(OssFileSystemParams.FS_URI) != null) {
				try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(factory.getClassLoader())) {
					loaded = factory.create(new Path(getParams().get(OssFileSystemParams.FS_URI)).toUri());
				}
				return loaded;
			} else if (path != null) {
				try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(factory.getClassLoader())) {
					loaded = factory.create(path.toUri());
				}
				return loaded;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		throw new RuntimeException("Could not create the oss file system. Both the bucket the filePath are null.");
	}

	@Override
	public FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(load(f).getClass()
			.getClassLoader())) {
			return super.create(f, overwriteMode);
		}
	}

	private FileSystemFactory createFactory() {
		if (classLoaderFactory == null) {
			classLoaderFactory = new FileSystemClassLoaderFactory(
				OSS_FILE_SYSTEM_NAME,
				getParams().get(OssFileSystemParams.PLUGIN_VERSION)
			);
		}

		try {
			return (FileSystemFactory) classLoaderFactory
				.create()
				.loadClass("org.apache.flink.fs.osshadoop.OSSFileSystemFactory")
				.getConstructor()
				.newInstance();
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
			ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getSchema() {
		return createFactory().getScheme();
	}
}