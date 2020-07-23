package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.fs.osshadoop.OSSFileSystemFactory;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.annotations.FSAnnotation;
import com.alibaba.alink.params.io.OssFileSystemParams;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@FSAnnotation(name = "oss")
public final class OssFileSystem extends BaseFileSystem<OssFileSystem> implements OssFileSystemParams<OssFileSystem> {

	public OssFileSystem(Params params) {
		super(params);
	}

	public OssFileSystem(String endPoint, String bucketName, String accessId, String accessKey) {
		this(endPoint, bucketName, accessId, accessKey, null);
	}

	public OssFileSystem(String endPoint, String bucketName, String accessId, String accessKey, String securityToken) {
		this(new Params());

		try {
			getParams()
				.set(OssFileSystemParams.END_POINT, endPoint)
				.set(OssFileSystemParams.ACCESS_ID, accessId)
				.set(OssFileSystemParams.ACCESS_KEY, accessKey)
				.set(OssFileSystemParams.SECURITY_TOKEN, securityToken)
				.set(
					OssFileSystemParams.FS_URI,
					new URI(getSchema(), bucketName, null, null).toString()
				);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected FileSystem load(Path path) {
		final Configuration conf = new Configuration();
		conf.setString("fs.oss.endpoint", get(OssFileSystemParams.END_POINT));

		if (getParams().get(OssFileSystemParams.ACCESS_ID) != null
			&& getParams().get(OssFileSystemParams.ACCESS_KEY) != null) {
			conf.setString("fs.oss.accessKeyId", get(OssFileSystemParams.ACCESS_ID));
			conf.setString("fs.oss.accessKeySecret", get(OssFileSystemParams.ACCESS_KEY));

			if (getParams().get(OssFileSystemParams.SECURITY_TOKEN) != null) {
				conf.setString("fs.oss.securityToken", get(OssFileSystemParams.SECURITY_TOKEN));
			}
		}

		OSSFileSystemFactory factory = new OSSFileSystemFactory();
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

		throw new RuntimeException("Could not create the oss file system. Both the bucket the filePath are null.");
	}

	@Override
	public String getSchema() {
		return new OSSFileSystemFactory().getScheme();
	}
}