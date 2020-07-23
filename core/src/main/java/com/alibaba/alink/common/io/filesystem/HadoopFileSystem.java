package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.runtime.fs.hdfs.HadoopFsFactory;

import com.alibaba.alink.common.io.annotations.FSAnnotation;
import com.alibaba.alink.params.io.HadoopFileSystemParams;

import java.io.IOException;

@FSAnnotation(name = "hadoop")
public final class HadoopFileSystem extends BaseFileSystem<HadoopFileSystem>
	implements HadoopFileSystemParams<HadoopFileSystem> {

	public HadoopFileSystem(Params params) {
		super(params);
	}

	public HadoopFileSystem(String fsUri) {
		this(new Params().set(HadoopFileSystemParams.FS_URI, fsUri));
	}

	@Override
	public String getSchema() {
		return new HadoopFsFactory().getScheme();
	}

	@Override
	protected FileSystem load(Path path) {
		final Configuration conf = new Configuration();
		HadoopFsFactory factory = new HadoopFsFactory();
		factory.configure(conf);

		try {
			if (getFSUri() != null) {
				return factory.create(new Path(getFSUri()).toUri());
			} else if (path != null) {
				return factory.create(path.toUri());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		throw new RuntimeException("Could not create the hadoop file system. Both the fsUri the filePath are null.");
	}
}
