package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelData;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelDataConverter;

import java.util.List;

public class NaiveBayesTextModelInfoBatchOp extends
	ExtractModelInfoBatchOp <NaiveBayesTextModelInfo, NaiveBayesTextModelInfoBatchOp> {

	private static final long serialVersionUID = 7879752165579171916L;

	public NaiveBayesTextModelInfoBatchOp() {
		this(null);
	}

	public NaiveBayesTextModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected NaiveBayesTextModelInfo createModelInfo(List <Row> rows) {
		NaiveBayesTextModelData modelData = new NaiveBayesTextModelDataConverter().load(rows);
		NaiveBayesTextModelInfo modelInfo = new NaiveBayesTextModelInfo(modelData.theta,
			modelData.pi, modelData.labels, modelData.vectorSize,
			modelData.vectorColName, modelData.modelType.toString());
		//        NaiveBayesTextModelInfo modelInfo = new NaiveBayesTextModelInfo(rows);
		return modelInfo;
	}
}
