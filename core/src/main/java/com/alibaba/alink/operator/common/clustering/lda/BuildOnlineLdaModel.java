package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.clustering.LdaModelData;
import com.alibaba.alink.params.clustering.LdaTrainParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Build online lda model.
 */
public class BuildOnlineLdaModel extends CompleteResultFunction {
	private static final long serialVersionUID = 6377566517589659547L;
	private int topicNum;
	private double beta;

	/**
	 * Constructor.
	 *
	 * @param topicNum the number of topics.
	 * @param beta     the beta param.
	 */
	public BuildOnlineLdaModel(int topicNum, double beta) {
		this.topicNum = topicNum;
		this.beta = beta;
	}

	@Override
	public List <Row> calc(ComContext context) {
		if (context.getTaskId() != 0) {
			return null;
		}
		int vocabularySize = ((List <Tuple2 <Long, Integer>>) context.getObj(LdaVariable.shape)).get(0).f1;

		DenseMatrix alpha = context.getObj(LdaVariable.alpha);

		LdaModelData modelData = new LdaModelData(topicNum, vocabularySize,
			new double[alpha.numRows()], new double[topicNum], context.getObj(LdaVariable.lambda));
		for (int i = 0; i < alpha.numRows(); i++) {
			modelData.alpha[i] = alpha.get(i, 0);
		}
		Arrays.fill(modelData.beta, beta);
		modelData.optimizer = LdaTrainParams.Method.Online;

		long nonEmptyWordCount = Math.round(((double[]) context.getObj(LdaVariable.nonEmptyWordCount))[0]);
		double[] logLikelihoods = context.getObj(LdaVariable.logLikelihood);
		modelData.logLikelihood = logLikelihoods[0];
		modelData.logPerplexity = -modelData.logLikelihood / nonEmptyWordCount;
		List <Row> res = new ArrayList <>();
		res.add(Row.of(modelData));
		return res;
	}
}
