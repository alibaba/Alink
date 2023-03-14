package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.annotations.FSAnnotation;
import com.alibaba.alink.params.io.HasIoName;

@FSAnnotation(name = "local")
public final class LocalFileSystem extends BaseFileSystem <LocalFileSystem> {
	private static final long serialVersionUID = 1818806211030723090L;

	public LocalFileSystem() {
		this(new Params());
	}

	public LocalFileSystem(Params params) {
		super(params.set(HasIoName.IO_NAME, LocalFileSystem.class.getAnnotation(FSAnnotation.class).name()));
	}

	@Override
	public String getSchema() {
		return com.alibaba.alink.common.io.filesystem.copy.local.LocalFileSystem.getLocalFsURI().getScheme();
	}

	@Override
	protected FileSystem load(Path path) {
		return com.alibaba.alink.common.io.filesystem.copy.local.LocalFileSystem.getSharedInstance();
	}

}
