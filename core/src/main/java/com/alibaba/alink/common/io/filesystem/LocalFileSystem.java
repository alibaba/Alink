package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystemFactory;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.annotations.FSAnnotation;

import java.io.IOException;

@FSAnnotation(name = "local")
public final class LocalFileSystem extends BaseFileSystem <LocalFileSystem> {
	private static final long serialVersionUID = 1818806211030723090L;
	private transient FileSystemFactory loaded;

	public LocalFileSystem() {
		this(new Params());
	}

	public LocalFileSystem(Params params) {
		super(params);
	}

	@Override
	public String getSchema() {
		return new LocalFileSystemFactory().getScheme();
	}

	@Override
	protected FileSystem load(Path path) {
		if (loaded == null) {
			loaded = new LocalFileSystemFactory();
		}

		try {
			return loaded.create(null);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException("local file load error",e);
		}
	}
}
