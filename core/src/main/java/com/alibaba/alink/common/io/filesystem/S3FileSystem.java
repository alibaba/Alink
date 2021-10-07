package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.filesystem.plugin.FileSystemClassLoaderFactory;
import com.alibaba.alink.common.io.filesystem.plugin.FileSystemClassLoaderFactory.S3FileSystemClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.params.io.S3FileSystemParams;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public abstract class S3FileSystem<T extends S3FileSystem <T>> extends BaseFileSystem <T> {

	private FileSystemClassLoaderFactory classLoaderFactory;
	private transient FileSystem loaded;

	public S3FileSystem(Params params) {
		super(params);
	}

	public S3FileSystem(
		String s3Version, String endPoint, String bucketName, String accessKey,
		String secretKey, boolean pathStyleAccess) {

		this(new Params());

		try {
			getParams()
				.set(S3FileSystemParams.PLUGIN_VERSION, s3Version)
				.set(S3FileSystemParams.END_POINT, endPoint)
				.set(S3FileSystemParams.ACCESS_KEY, accessKey)
				.set(S3FileSystemParams.SECRET_KEY, secretKey)
				.set(S3FileSystemParams.PATH_STYLE_ACCESS, pathStyleAccess)
				.set(
					S3FileSystemParams.FS_URI,
					new URI(getSchema(), bucketName, null, null).toString()
				);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(e);
		}
	}

	protected abstract String getPluginName();

	private List <FileSystemFactory> createFactory() {
		if (classLoaderFactory == null) {
			classLoaderFactory = new S3FileSystemClassLoaderFactory(
				getPluginName(),
				getParams().get(S3FileSystemParams.PLUGIN_VERSION)
			);
		}

		ClassLoader classLoader = classLoaderFactory.create();

		ArrayList <FileSystemFactory> factories = new ArrayList <>();

		ServiceLoader.load(FileSystemFactory.class, classLoader).forEach(factories::add);

		return factories;
	}

	@Override
	protected FileSystem load(Path path) {
		if (loaded != null) {
			return loaded;
		}

		final Configuration conf = new Configuration();

		conf.setString("s3.endpoint", getParams().get(S3FileSystemParams.END_POINT));

		if (getParams().get(S3FileSystemParams.ACCESS_KEY) != null
			&& getParams().get(S3FileSystemParams.SECRET_KEY) != null) {

			conf.setString("s3.access-key", getParams().get(S3FileSystemParams.ACCESS_KEY));
			conf.setString("s3.secret-key", getParams().get(S3FileSystemParams.SECRET_KEY));
		}

		conf.setString("s3.path.style.access", getParams().get(S3FileSystemParams.PATH_STYLE_ACCESS).toString());

		List <FileSystemFactory> factories = createFactory();

		Path localPath = path;

		if (getParams().get(S3FileSystemParams.FS_URI) != null) {
			localPath = new Path(getParams().get(S3FileSystemParams.FS_URI));
		}

		for (FileSystemFactory factory : factories) {

			factory.configure(conf);

			if (localPath.toUri().getScheme().equals(factory.getScheme())) {
				try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(factory.getClassLoader())) {
					loaded = factory.create(localPath.toUri());
					return loaded;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

		throw new IllegalStateException("Could not find the file system of " + localPath.toString());
	}
}
