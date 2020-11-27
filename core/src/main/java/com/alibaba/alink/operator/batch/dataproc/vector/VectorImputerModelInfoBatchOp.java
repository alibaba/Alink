package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.dataproc.ImputerModelInfoBatchOp;
import com.alibaba.alink.operator.common.dataproc.ImputerModelInfo;
import com.alibaba.alink.params.dataproc.ImputerTrainParams;
import com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams;

import java.util.List;

public class VectorImputerModelInfoBatchOp
	extends ExtractModelInfoBatchOp <ImputerModelInfo, VectorImputerModelInfoBatchOp> {

	private static final long serialVersionUID = 8092004475546948679L;

	public VectorImputerModelInfoBatchOp() {
		this(null);
	}

	public VectorImputerModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected ImputerModelInfo createModelInfo(List <Row> rows) {
		ImputerModelInfo imputerModelInfo = new ImputerModelInfo(rows);
		imputerModelInfo.vectorCol = this.getParams().get(VectorImputerTrainParams.SELECTED_COL);
		return imputerModelInfo;
	}
}


