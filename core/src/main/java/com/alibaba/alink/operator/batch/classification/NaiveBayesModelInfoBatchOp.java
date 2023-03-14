package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelData;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelDataConverter;

import java.util.List;

public class NaiveBayesModelInfoBatchOp
	extends ExtractModelInfoBatchOp <NaiveBayesModelInfo, NaiveBayesModelInfoBatchOp> {

	private static final long serialVersionUID = -4397159591959699351L;

	public NaiveBayesModelInfoBatchOp() {
		this(new Params());
	}

	public NaiveBayesModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public NaiveBayesModelInfo createModelInfo(List <Row> rows) {
		NaiveBayesModelData modelData = new NaiveBayesModelDataConverter().load(rows);
		NaiveBayesModelInfo modelInfo = new NaiveBayesModelInfo(modelData.featureNames,
			modelData.isCate,
			modelData.labelWeights,
			modelData.label,
			modelData.weightSum,
			modelData.featureInfo,
			modelData.stringIndexerModelSerialized);

		return modelInfo;
	}

}
