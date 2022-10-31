package com.alibaba.alink.pipeline;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.AkSourceLocalOp;

class ModelPipeFileData {
	private final FilePath modelFilePath;
	AkSourceBatchOp sourceBatch = null;
	AkSourceLocalOp sourceLocal = null;

	public ModelPipeFileData(FilePath modelFilePath) {
		this.modelFilePath = modelFilePath;
	}

	public BatchOperator <?> getBatchData() {
		if (null == this.sourceBatch) {
			this.sourceBatch = new AkSourceBatchOp().setFilePath(this.modelFilePath);
		}
		return this.sourceBatch;
	}

	public LocalOperator <?> getLocalData() {
		if (null == this.sourceLocal) {
			this.sourceLocal = new AkSourceLocalOp().setFilePath(this.modelFilePath);
		}
		return this.sourceLocal;
	}

}
