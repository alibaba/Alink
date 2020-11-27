package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.special.Gamma;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;

/**
 * Lda util.
 */
public class LdaUtil {

	/**
	 * Calculate digamma of the input value.
	 */
	public static double digamma(double x) {
		return Gamma.digamma(x);
	}

	/**
	 * Calculate digamma of each value of the input matrix.
	 */
	private static DenseMatrix digamma(DenseMatrix x) {
		int rowNum = x.numRows();
		int colNum = x.numCols();
		DenseMatrix dmr = new DenseMatrix(rowNum, colNum);
		MatVecOp.apply(x, dmr, LdaUtil::digamma);
		return dmr;
	}

	/**
	 * Calculate trigamma of the input value.
	 */
	static double trigamma(double x) {
		return Gamma.trigamma(x);
	}

	/**
	 * Calculate trigamma of each value of the input matrix.
	 */
	static DenseMatrix trigamma(DenseMatrix x) {
		int rowNum = x.numRows();
		int colNum = x.numCols();
		DenseMatrix dmr = new DenseMatrix(rowNum, colNum);
		MatVecOp.apply(x, dmr, LdaUtil::trigamma);
		return dmr;
	}

	/**
	 * Calculate logGamma of the input value.
	 */
	static double logGamma(double x) {
		return Gamma.logGamma(x);
	}

	/**
	 * Calculate logGamma of each value of the input matrix.
	 */
	static DenseMatrix logGamma(DenseMatrix x) {
		int rowNum = x.numRows();
		int colNum = x.numCols();
		DenseMatrix dmr = new DenseMatrix(rowNum, colNum);
		MatVecOp.apply(x, dmr, LdaUtil::logGamma);
		return dmr;
	}

	/**
	 * Calculate the dirichlet expectation.
	 */
	static DenseMatrix dirichletExpectation(DenseMatrix alpha) {
		DenseMatrix rowSum = sumByCol(alpha);
		DenseMatrix digAlpha = digamma(alpha);
		DenseMatrix digRowSum = digamma(rowSum);
		for (int j = 0; j < alpha.numCols(); j++) {
			for (int i = 0; i < alpha.numRows(); i++) {
				digAlpha.set(i, j, digAlpha.get(i, j) - digRowSum.get(0, i));
			}
		}
		return digAlpha;
	}

	/**
	 * Calculate the dirichlet expectation of vector.
	 */
	static DenseMatrix dirichletExpectationVec(DenseMatrix alpha) {
		DenseMatrix x = digamma(alpha);
		x.plusEquals(-digamma(alpha.sum()));
		return x;
	}

	/**
	 * Calculate exp of each value of the input matrix.
	 */
	public static void exp(DenseMatrix dm) {
		double[] arrayData = dm.getData();
		for (int i = 0; i < arrayData.length; i++) {
			arrayData[i] = Math.exp(arrayData[i]);
		}
	}

	/**
	 * Calculate the exp dirichlet expectation under theta.
	 */
	public static DenseMatrix expDirichletExpectation(DenseMatrix alpha) {
		DenseMatrix expDigAlpha = dirichletExpectation(alpha);
		exp(expDigAlpha);
		return expDigAlpha;
	}

	/**
	 * Calculate word-topic probability information.
	 */
	public static double[] getTopicDistributionMethod(SparseVector sv,
													  DenseMatrix expELogBeta,
													  DenseMatrix alphaMatrix,
													  int topicNum, int gammaShape, RandomDataGenerator random) {

		return getTopicDistributionMethod(sv, expELogBeta, alphaMatrix, geneGamma(topicNum, gammaShape, random),
			topicNum).f0.getColumn(0);
	}

	/**
	 * Generate DenseMatrix whose values are all belong to Gamma distribution.
	 */
	static DenseMatrix geneGamma(int numTopic, int gammaShape, RandomDataGenerator rand) {
		double[] gammaVec = new double[numTopic];
		for (int i = 0; i < numTopic; i++) {
			gammaVec[i] = rand.nextGamma(gammaShape, 1.0 / gammaShape);
		}
		return LdaUtil.vectorToMatrix(gammaVec);
	}

	/**
	 * this function executes the repeat process in the Algorithm.
	 * it operates on one input doc, and return its word-topic probability information.
	 */
	static Tuple2 <DenseMatrix, DenseMatrix> getTopicDistributionMethod(SparseVector sv,
																		DenseMatrix expELogBeta,
																		DenseMatrix alphaMatrix,
																		DenseMatrix gammad,
																		int topicNum) {
		if (sv.numberOfValues() == 0) {
			return new Tuple2 <>(DenseMatrix.zeros(1, topicNum), DenseMatrix.zeros(1, topicNum));
		}
		DenseMatrix cts = vectorToMatrix(sv.getValues());
		DenseMatrix expELogThetad = dirichletExpectationVec(gammad);
		exp(expELogThetad);//topic distribution of current doc.
		//calculate expELogBetad.
		DenseMatrix expELogBetad = expELogBeta.selectRows(
			sv.getIndices());//the word-topic distribution of word in this doc.
		DenseMatrix phiNorm = expELogBetad.multiplies(expELogThetad);//
		phiNorm.plusEquals(1e-100);//may be the probability of each word appears.
		double meanGammaChange = 1.0;
		DenseMatrix oldGammad;
		//update theta and Gamma, until it stays unchangeable.
		while ((meanGammaChange > 1e-3)) {
			oldGammad = gammad.clone();
			gammad = LdaUtil.elementWiseProduct(expELogThetad, expELogBetad.transpose()
				.multiplies(LdaUtil.elementWiseDivide(cts, phiNorm)));
			gammad.plusEquals(alphaMatrix);
			//update expELogThetad with gammad.
			expELogThetad = dirichletExpectationVec(gammad);
			exp(expELogThetad);
			phiNorm = expELogBetad.multiplies(expELogThetad);
			phiNorm.plusEquals(1e-100);
			meanGammaChange = diffDenseMatrix(gammad, oldGammad, topicNum);
		}
		DenseMatrix wordTopicStat = expELogThetad.multiplies(
			LdaUtil.elementWiseDivide(cts, phiNorm).transpose());//topic-word distribution of this doc.
		return new Tuple2 <>(gammad, wordTopicStat);
	}

	/**
	 * transform a vector to a matrix with size (vecLength, 1)
	 */
	public static DenseMatrix vectorToMatrix(double[] vec) {
		DenseMatrix dm = new DenseMatrix(vec.length, 1);
		for (int i = 0; i < vec.length; i++) {
			dm.set(i, 0, vec[i]);
		}
		return dm;
	}

	/**
	 * calculate the difference between A and B.
	 */
	private static double diffDenseMatrix(DenseMatrix A, DenseMatrix B, int topicNum) {
		double diff = 0;
		for (int j = 0; j < A.numCols(); j++) {
			for (int i = 0; i < A.numRows(); i++) {
				diff += Math.abs(A.get(i, j) - B.get(i, j));
			}
		}
		return diff / topicNum;
	}

	/**
	 * matC := matA .* matB .
	 */
	static DenseMatrix elementWiseProduct(DenseMatrix matA, DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(matA.numRows(), matA.numCols());
		MatVecOp.apply(matA, matB, matC, ((a, b) -> a * b));
		return matC;
	}

	/**
	 * matC := matA ./ matB.
	 */
	static DenseMatrix elementWiseDivide(DenseMatrix matA, DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(matA.numRows(), matA.numCols());
		MatVecOp.apply(matA, matB, matC, ((a, b) -> a / b));
		return matC;
	}

	/**
	 * Create a 1 x m matrix by summing each of the columns of a m x n matrix.
	 */
	private static DenseMatrix sumByCol(DenseMatrix mat) {
		int m = mat.numRows();
		int n = mat.numCols();
		DenseMatrix rowSums = new DenseMatrix(1, m);
		for (int i = 0; i < m; i++) {
			double s = 0.;
			for (int j = 0; j < n; j++) {
				s += mat.get(i, j);
			}
			rowSums.set(0, i, s);
		}
		return rowSums;
	}

	/**
	 * Create a 1 x n matrix by summing each of the rows of a m x n matrix.
	 */
	static DenseMatrix sumByRow(DenseMatrix mat) {
		int m = mat.numRows();
		int n = mat.numCols();
		DenseMatrix colSums = new DenseMatrix(1, n);
		for (int i = 0; i < n; i++) {
			double s = 0.;
			for (int j = 0; j < m; j++) {
				s += mat.get(j, i);
			}
			colSums.set(0, i, s);
		}
		return colSums;
	}

	static double logSumExp(DenseMatrix dm) {
		double max = dm.get(0, 0);
		for (int i = 0; i < dm.numRows(); i++) {
			for (int j = 0; j < dm.numCols(); j++) {
				if (dm.get(i, j) > max) {
					max = dm.get(i, j);
				}
			}
		}
		double sum = 0;
		for (int i = 0; i < dm.numRows(); i++) {
			for (int j = 0; j < dm.numCols(); j++) {
				sum += Math.exp(dm.get(i, j) - max);
			}
		}
		return max + Math.log(sum);
	}

	/**
	 * Transform the input SparseVector data to HashMap in the train process.
	 */
	public static HashMap <Integer, String> setWordIdWeightTrain(List <String> list) {
		int hashMapLength = list.size();
		final Type dataTuple3Type = new TypeReference <Tuple3 <String, Double, Integer>>() {}.getType();
		HashMap <Integer, String> wordIdWeight = new HashMap <>(hashMapLength);
		for (String feature : list) {
			Tuple3 <String, Double, Integer> t = JsonConverter.fromJson(feature, dataTuple3Type);
			wordIdWeight.put(t.f2, t.f0);
		}
		return wordIdWeight;
	}

	/**
	 * Transform the input SparseVector data to HashMap in the predict process.
	 */
	public static HashMap <String, Tuple2 <Integer, Double>> setWordIdWeightPredict(List <String> list) {
		int hashMapLength = list.size();
		final Type dataTuple3Type = new TypeReference <Tuple3 <String, Double, Integer>>() {}.getType();
		HashMap <String, Tuple2 <Integer, Double>> wordIdWeight = new HashMap <>(hashMapLength);
		for (String feature : list) {
			Tuple3 <String, Double, Integer> t = JsonConverter.fromJson(feature, dataTuple3Type);
			wordIdWeight.put(t.f0, Tuple2.of(t.f2, t.f1));
		}
		return wordIdWeight;
	}

}
