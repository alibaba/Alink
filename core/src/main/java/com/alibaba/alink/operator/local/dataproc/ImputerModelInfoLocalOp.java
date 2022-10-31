package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.ImputerModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.ImputerTrainParams;

import java.util.List;

public class ImputerModelInfoLocalOp
	extends ExtractModelInfoLocalOp <ImputerModelInfo, ImputerModelInfoLocalOp> {

	public ImputerModelInfoLocalOp() {
		this(null);
	}

	public ImputerModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected ImputerModelInfo createModelInfo(List <Row> rows) {
		ImputerModelInfo imputerModelInfo = new ImputerModelInfo(rows);
		imputerModelInfo.selectedCols = this.getParams().get(ImputerTrainParams.SELECTED_COLS);
		return imputerModelInfo;
	}
}
