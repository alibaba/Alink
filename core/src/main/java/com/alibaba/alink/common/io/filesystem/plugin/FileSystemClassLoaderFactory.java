package com.alibaba.alink.common.io.filesystem.plugin;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystemFactory;

import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.PluginDescriptor;
import com.alibaba.alink.common.io.plugin.RegisterKey;

import java.util.function.Function;
import java.util.function.Predicate;

public class FileSystemClassLoaderFactory extends ClassLoaderFactory {

	private static final long serialVersionUID = 6388056534023187796L;

	public FileSystemClassLoaderFactory(String name, String version) {
		super(new RegisterKey(name, version), PluginDistributeCache.createDistributeCache(name, version));
	}

	@Override
	public ClassLoader create() {
		return ClassLoaderContainer.getInstance().create(
			registerKey,
			distributeCache,
			FileSystemFactory.class,
			new FileSystemServiceFilter(registerKey),
			new FileSystemVersionGetter()
		);
	}

	private static class FileSystemServiceFilter implements Predicate<FileSystemFactory> {
		private final RegisterKey registerKey;

		public FileSystemServiceFilter(RegisterKey registerKey) {
			this.registerKey = registerKey;
		}

		@Override
		public boolean test(FileSystemFactory factory) {
			return factory.getScheme().equals(registerKey.getName());
		}
	}

	private static class FileSystemVersionGetter implements Function<Tuple2<FileSystemFactory, PluginDescriptor>, String> {
		@Override
		public String apply(Tuple2<FileSystemFactory, PluginDescriptor> factory) {
			return factory.f1.getVersion();
		}
	}

	public static class HadoopFileSystemClassLoaderFactory extends FileSystemClassLoaderFactory {

		private static final long serialVersionUID = -3820737055851955229L;

		public HadoopFileSystemClassLoaderFactory(String name, String version) {
			super(name, version);
		}

		@Override
		public ClassLoader create() {
			return ClassLoaderContainer.getInstance().create(
				registerKey,
				distributeCache,
				FileSystemFactory.class,
				new HadoopFileSystemServiceFilter(),
				new FileSystemVersionGetter()
			);
		}
	}

	private static class HadoopFileSystemServiceFilter implements Predicate<FileSystemFactory> {
		public HadoopFileSystemServiceFilter() {
		}

		@Override
		public boolean test(FileSystemFactory factory) {
			return factory.getScheme().equals("*");
		}
	}

	public static class S3FileSystemClassLoaderFactory extends FileSystemClassLoaderFactory {

		private static final long serialVersionUID = -3820737055851955229L;

		public S3FileSystemClassLoaderFactory(String name, String version) {
			super(name, version);
		}

		@Override
		public ClassLoader create() {
			return ClassLoaderContainer.getInstance().create(
				registerKey,
				distributeCache,
				FileSystemFactory.class,
				new S3FileSystemServiceFilter(),
				new FileSystemVersionGetter()
			);
		}
	}

	private static class S3FileSystemServiceFilter implements Predicate <FileSystemFactory> {
		public S3FileSystemServiceFilter() {
		}

		@Override
		public boolean test(FileSystemFactory factory) {
			String schema = factory.getScheme();

			return schema.equals("s3") || schema.equals("s3a") || schema.equals("s3p");
		}
	}
}
