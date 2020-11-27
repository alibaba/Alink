package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.dataproc.ImputerModelInfo;
import com.alibaba.alink.params.dataproc.ImputerTrainParams;

import java.util.List;

public class ImputerModelInfoBatchOp
	extends ExtractModelInfoBatchOp<ImputerModelInfo, ImputerModelInfoBatchOp> {

	private static final long serialVersionUID = 8092004475546948679L;

	public ImputerModelInfoBatchOp() {
		this(null);
	}

	public ImputerModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected ImputerModelInfo createModelInfo(List<Row> rows) {
		ImputerModelInfo imputerModelInfo =  new ImputerModelInfo(rows);
		imputerModelInfo.selectedCols = this.getParams().get(ImputerTrainParams.SELECTED_COLS);
		return imputerModelInfo;
	}
}
