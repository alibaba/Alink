package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.nlp.Word2VecTrainParams;

import java.util.List;

public class Word2VecTrainInfo {
	public static final ParamInfo<Double[]> LOSS = ParamInfoFactory
		.createParamInfo("loss", Double[].class)
		.build();

	public static final ParamInfo<Long> NUM_VOCAB = ParamInfoFactory
		.createParamInfo("numVocab", Long.class)
		.build();

	private Params params;

	public Word2VecTrainInfo(List<Row> rows) {
		Preconditions.checkArgument(
			rows != null && rows.size() == 1 && rows.get(0) != null && rows.get(0).getArity() == 1,
			"Invalid word2vec model summary."
		);
		params = Params.fromJson((String) rows.get(0).getField(0));
	}

	public int getNumIter() {
		return params.get(Word2VecTrainParams.NUM_ITER);
	}

	public Double[] getLoss() {
		return params.get(LOSS);
	}

	public Long getNumVocab() {
		return params.get(NUM_VOCAB);
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();

		stringBuilder.append("Word2Vec model summary: ").append("\n");
		stringBuilder.append("NumIter: ").append(getNumIter()).append("\n");
		stringBuilder.append("NumVocab: ").append(getNumVocab()).append("\n");
		stringBuilder.append("LossInfo: ").append("\n");

		Double[] loss = getLoss();
		Object[][] lossTable = new Object[loss.length][];

		for (int i = 0; i < loss.length; ++i) {
			lossTable[i] = new Object[]{i, loss[i]};
		}

		stringBuilder.append(
			PrettyDisplayUtils.displayTable(lossTable, loss.length, 2, null, new String[]{"Batch", "Loss"}, null)
		);

		return stringBuilder.toString();
	}
}
