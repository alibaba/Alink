package com.alibaba.alink.operator.common.aps.checkpoint;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;

public final class ApsAkCheckpoint extends ApsCheckpoint {
	private final BaseFileSystem <?> fileSystem;

	public ApsAkCheckpoint(BaseFileSystem <?> fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public void write(BatchOperator <?> operator, String identity, Long mlEnvId, Params params) {
		operator.link(
			new AkSinkBatchOp()
				.setFilePath(new FilePath(identity, fileSystem))
				.setMLEnvironmentId(mlEnvId)
		);
	}

	@Override
	public BatchOperator <?> read(String identity, Long mlEnvId, Params params) {
		return new AkSourceBatchOp()
			.setFilePath(new FilePath(identity, fileSystem))
			.setMLEnvironmentId(mlEnvId);
	}
}
