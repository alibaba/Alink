package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.ImputerModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams;

import java.util.List;

public class VectorImputerModelInfoLocalOp
	extends ExtractModelInfoLocalOp <ImputerModelInfo, VectorImputerModelInfoLocalOp> {

	public VectorImputerModelInfoLocalOp() {
		this(null);
	}

	public VectorImputerModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected ImputerModelInfo createModelInfo(List <Row> rows) {
		ImputerModelInfo imputerModelInfo = new ImputerModelInfo(rows);
		imputerModelInfo.vectorCol = this.getParams().get(VectorImputerTrainParams.SELECTED_COL);
		return imputerModelInfo;
	}
}


