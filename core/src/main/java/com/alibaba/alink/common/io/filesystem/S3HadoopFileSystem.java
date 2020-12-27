package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.annotations.FSAnnotation;

@FSAnnotation(name = "s3_hadoop")
public class S3HadoopFileSystem extends S3FileSystem <S3HadoopFileSystem> {

	public final static String S3_HADOOP_FILE_SYSTEM_NAME = "s3-hadoop";

	public S3HadoopFileSystem(Params params) {
		super(params);
	}

	public S3HadoopFileSystem(
		String s3HadoopVersion, String endPoint, String bucketName, String accessKey,
		String secretKey, boolean pathStyleAccess) {

		super(s3HadoopVersion, endPoint, bucketName, accessKey, secretKey, pathStyleAccess);
	}

	@Override
	protected String getPluginName() {
		return S3_HADOOP_FILE_SYSTEM_NAME;
	}

	@Override
	public String getSchema() {
		return "s3a";
	}
}
