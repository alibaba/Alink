package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.annotations.FSAnnotation;
import com.alibaba.alink.params.io.FlinkFileSystemParams;

import java.io.IOException;

@FSAnnotation(name = "flink")
public class FlinkFileSystem extends BaseFileSystem <FlinkFileSystem> {

	private static final long serialVersionUID = -7868949732941104115L;

	public FlinkFileSystem(String fsUri) {
		super(new Params().set(FlinkFileSystemParams.FS_URI, fsUri));
	}

	public FlinkFileSystem(Params params) {
		super(params);
	}

	@Override
	public String getSchema() {
		throw new RuntimeException("Could not get the schema in flink file system.");
	}

	@Override
	protected FileSystem load(Path path) {
		try {
			if (getParams().get(FlinkFileSystemParams.FS_URI) != null) {
				return FileSystem.get(new Path(getParams().get(FlinkFileSystemParams.FS_URI)).toUri());
			} else if (path != null) {
				return FileSystem.get(path.toUri());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		throw new RuntimeException("Could not create the flink file system. Both the fsUri the filePath are null.");
	}
}
