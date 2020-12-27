package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.annotations.FSAnnotation;
import com.alibaba.alink.common.io.filesystem.plugin.FileSystemClassLoaderFactory;

@FSAnnotation(name = "s3_presto")
public class S3PrestoFileSystem extends S3FileSystem <S3PrestoFileSystem> {

	public final static String S3_PRESTO_FILE_SYSTEM_NAME = "s3-presto";

	private FileSystemClassLoaderFactory classLoaderFactory;
	private transient FileSystem loaded;

	public S3PrestoFileSystem(Params params) {
		super(params);
	}

	public S3PrestoFileSystem(
		String s3PrestoVersion, String endPoint, String bucketName, String accessKey,
		String secretKey, boolean pathStyleAccess) {

		super(s3PrestoVersion, endPoint, bucketName, accessKey, secretKey, pathStyleAccess);
	}

	@Override
	protected String getPluginName() {
		return S3_PRESTO_FILE_SYSTEM_NAME;
	}

	@Override
	public String getSchema() {
		return "s3p";
	}
}
