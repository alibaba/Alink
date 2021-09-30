package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.FileUtils;

import com.alibaba.alink.common.io.annotations.FSAnnotation;
import com.alibaba.alink.common.io.filesystem.plugin.FileSystemClassLoaderFactory;
import com.alibaba.alink.common.io.filesystem.plugin.FileSystemClassLoaderFactory.HadoopFileSystemClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.params.io.HadoopFileSystemParams;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;

@FSAnnotation(name = "hadoop")
public final class HadoopFileSystem extends BaseFileSystem <HadoopFileSystem> {
	private static final long serialVersionUID = 2454967720475785815L;

	public final static String HADOOP_FILE_SYSTEM_NAME = "hadoop";

	private FileSystemClassLoaderFactory classLoaderFactory;
	private transient FileSystem loaded;

	public HadoopFileSystem(Params params) {
		super(params);
	}

	public HadoopFileSystem(String hadoopVersion, String fsUri) {
		this(new Params()
			.set(HadoopFileSystemParams.PLUGIN_VERSION, hadoopVersion)
			.set(HadoopFileSystemParams.FS_URI, fsUri));
	}

	public HadoopFileSystem(String hadoopVersion, String fsUri, String configuration) {
		this(new Params()
			.set(HadoopFileSystemParams.PLUGIN_VERSION, hadoopVersion)
			.set(HadoopFileSystemParams.FS_URI, fsUri)
			.set(HadoopFileSystemParams.CONFIGURATION, configuration)
		);
	}

	public HadoopFileSystem(String hadoopVersion, String fsUri, FilePath configuration) {
		this(
			new Params()
				.set(HadoopFileSystemParams.PLUGIN_VERSION, hadoopVersion)
				.set(HadoopFileSystemParams.FS_URI, fsUri)
				.set(HadoopFileSystemParams.CONFIGURATION_FILE_PATH, configuration.serialize())
		);
	}

	@Override
	public String getSchema() {
		return createFactory().getScheme();
	}

	@Override
	protected FileSystem load(Path path) {

		if (loaded != null) {
			return loaded;
		}

		final Configuration configuration = new Configuration();

		try {
			if (getParams().contains(HadoopFileSystemParams.CONFIGURATION)) {
				final String hdfsSiteFileName = "hdfs-site.xml";
				configuration.setString(
					ConfigConstants.HDFS_SITE_CONFIG,
					new Path(
						write(getParams().get(HadoopFileSystemParams.CONFIGURATION), hdfsSiteFileName),
						hdfsSiteFileName
					).toString()
				);
			} else if (getParams().contains(HadoopFileSystemParams.CONFIGURATION_FILE_PATH)) {
				FilePath configurePath = FilePath.deserialize(
					getParams().get(HadoopFileSystemParams.CONFIGURATION_FILE_PATH)
				);

				configuration.setString(
					ConfigConstants.HDFS_SITE_CONFIG,
					new Path(
						FilePath.download(
							new FilePath(configurePath.getPath().getParent(), configurePath.getFileSystem()),
							configurePath.getPath().getName()
						),
						configurePath.getPath().getName()
					).toString()
				);
			}

			FileSystemFactory factory = createFactory();
			factory.configure(configuration);

			if (getParams().get(HadoopFileSystemParams.FS_URI) != null) {
				try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(factory.getClassLoader())) {
					loaded = factory.create(new Path(getParams().get(HadoopFileSystemParams.FS_URI)).toUri());
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

		throw new RuntimeException("Could not create the hadoop file system. Both the fsUri the filePath are null.");
	}

	private FileSystemFactory createFactory() {
		if (classLoaderFactory == null) {
			classLoaderFactory = new HadoopFileSystemClassLoaderFactory(
				HADOOP_FILE_SYSTEM_NAME,
				getParams().get(HadoopFileSystemParams.PLUGIN_VERSION)
			);
		}

		try {
			return (FileSystemFactory) classLoaderFactory
				.create()
				.loadClass("org.apache.flink.runtime.fs.hdfs.HadoopFsFactory")
				.getConstructor()
				.newInstance();
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
			ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}


	private static String write(String fileName, String content) throws IOException {

		File localConfDir = new File(System.getProperty("java.io.tmpdir"), FileUtils.getRandomFilename(""));

		if (!localConfDir.mkdir()) {
			throw new RuntimeException("Could not create the dir " + localConfDir.getAbsolutePath());
		}

		try (FileOutputStream outputStream =
				 new FileOutputStream(Paths.get(localConfDir.getPath(), fileName).toFile());
			 ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes())) {
			IOUtils.copy(inputStream, outputStream);
		}

		return localConfDir.getAbsolutePath();
	}
}
