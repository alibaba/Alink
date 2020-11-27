package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.List;

/**
 * Online corpus step.
 * Build the corpus, calculate the word-topic probability information,
 * logPhatPart, no empty word and document count.
 */
public class OnlineCorpusStep extends ComputeFunction {

	private static final long serialVersionUID = 8719846772171941445L;
	private int numTopic;
	private double subSamplingRate;
	private int gammaShape;
	private Integer seed;
	private RandomDataGenerator random = new RandomDataGenerator();
	private boolean addedIndex = false;

	/**
	 * Constructor.
	 *
	 * @param numTopic        the number of topics.
	 * @param subSamplingRate the rate of the doc which need to be sampled.
	 */
	public OnlineCorpusStep(int numTopic, double subSamplingRate, int gammaShape, Integer seed) {
		this.numTopic = numTopic;
		this.subSamplingRate = subSamplingRate;
		this.gammaShape = gammaShape;
		this.seed = seed;
	}

	@Override
	public void calc(ComContext context) {
		if (!addedIndex && seed != null) {
			random.reSeed(seed);
			addedIndex = true;
		}
		Tuple2 <Long, Integer> tuple2 = ((List <Tuple2 <Long, Integer>>) context.getObj(LdaVariable.shape)).get(0);
		int vocabularySize = tuple2.f1;
		List <Vector> data = context.getObj(LdaVariable.data);
		DenseMatrix lambda;
		DenseMatrix alpha;
		//the first iteration
		if (context.getStepNo() == 1) {
			Tuple2 <DenseMatrix, DenseMatrix> initGammaAndAlpha = ((List <Tuple2 <DenseMatrix, DenseMatrix>>)
				context.getObj(LdaVariable.initModel)).get(0);
			lambda = initGammaAndAlpha.f0;
			alpha = initGammaAndAlpha.f1;
		} else {
			lambda = context.getObj(LdaVariable.lambda);
			alpha = context.getObj(LdaVariable.alpha);
		}
		if (data == null || data.size() == 0) {
			context.putObj(LdaVariable.wordTopicStat, new double[numTopic * vocabularySize]);
			context.putObj(LdaVariable.logPhatPart, new double[numTopic]);
			context.putObj(LdaVariable.nonEmptyWordCount, new double[] {0});
			context.putObj(LdaVariable.nonEmptyDocCount, new double[] {0});
			return;
		}
		DenseMatrix gammad = null;
		Tuple4 <DenseMatrix, DenseMatrix, Long, Long> corpusUpdatedData =
			onlineCorpusUpdate(data, lambda, alpha, gammad,
				vocabularySize, numTopic, subSamplingRate, random, gammaShape);
		context.putObj(LdaVariable.wordTopicStat, corpusUpdatedData.f0.getData().clone());
		context.putObj(LdaVariable.logPhatPart, corpusUpdatedData.f1.getData().clone());
		context.putObj(LdaVariable.nonEmptyWordCount, new double[] {corpusUpdatedData.f2});
		context.putObj(LdaVariable.nonEmptyDocCount, new double[] {corpusUpdatedData.f3});
	}

	public static Tuple4 <DenseMatrix, DenseMatrix, Long, Long> onlineCorpusUpdate(
		List <Vector> data, DenseMatrix lambda, DenseMatrix alpha, DenseMatrix gammad,
		int vocabularySize, int numTopic, double subSamplingRate, RandomDataGenerator random, int gammaShape) {

		DenseMatrix wordTopicStat = DenseMatrix.zeros(numTopic, vocabularySize);
		DenseMatrix logPhatPart = new DenseMatrix(numTopic, 1);

		DenseMatrix expELogBeta = LdaUtil.expDirichletExpectation(lambda).transpose();
		long nonEmptyWordCount = 0;
		long nonEmptyDocCount = 0;
		//the online corpus update stage can update the model in two way.
		//if the document order is determined, then it will update in the order.
		//or it will choose documents randomly.
		int dataSize = data.size();
		int[] indices = generateOnlineDocs(dataSize, subSamplingRate, random);
		for (int index : indices) {
			Vector vec = data.get(index);
			SparseVector sv = (SparseVector) vec;
			sv.setSize(vocabularySize);
			sv.removeZeroValues();
			for (int i = 0; i < sv.numberOfValues(); i++) {
				nonEmptyWordCount += sv.getValues()[i];
			}

			gammad = LdaUtil.geneGamma(numTopic, gammaShape, random);

			Tuple2 <DenseMatrix, DenseMatrix> topicDistributionTuple =
				LdaUtil.getTopicDistributionMethod(sv, expELogBeta, alpha, gammad, numTopic);

			for (int i = 0; i < sv.getIndices().length; i++) {
				for (int k = 0; k < numTopic; k++) {
					wordTopicStat.add(k, sv.getIndices()[i], topicDistributionTuple.f1.get(k, i));
				}
			}

			gammad = topicDistributionTuple.f0;
			DenseMatrix deGammad = LdaUtil.dirichletExpectationVec(gammad);
			for (int k = 0; k < numTopic; k++) {
				logPhatPart.add(k, 0, deGammad.get(k, 0));
			}
			nonEmptyDocCount++;
		}
		return new Tuple4 <>(wordTopicStat, logPhatPart, nonEmptyWordCount, nonEmptyDocCount);
	}

	private static int[] generateOnlineDocs(int length, double subSamplingRate, RandomDataGenerator random) {
		int sampleNum = (int) Math.ceil(length * subSamplingRate);
		int[] indices = new int[sampleNum];
		for (int i = 0; i < sampleNum; i++) {
			indices[i] = random.nextInt(0, length - 1);
		}
		return indices;
	}
}
