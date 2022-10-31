package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.classification.NaiveBayesModelInfo;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelData;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelDataConverter;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class NaiveBayesModelInfoLocalOp
	extends ExtractModelInfoLocalOp <NaiveBayesModelInfo, NaiveBayesModelInfoLocalOp> {

	public NaiveBayesModelInfoLocalOp() {
		this(new Params());
	}

	public NaiveBayesModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected NaiveBayesModelInfo createModelInfo(List <Row> rows) {
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
