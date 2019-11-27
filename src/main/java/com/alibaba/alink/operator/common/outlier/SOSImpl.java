package com.alibaba.alink.operator.common.outlier;

import com.alibaba.alink.common.linalg.MatVecOp;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.outlier.SosParams;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of the Stochastic Outlier Selection algorithm by Jeroen Jansen
 * <p>
 * For more information about SOS, see https://github.com/jeroenjanssens/sos
 * J.H.M. Janssens, F. Huszar, E.O. Postma, and H.J. van den Herik. Stochastic
 * Outlier Selection. Technical Report TiCC TR 2012-001, Tilburg University,
 * Tilburg, the Netherlands, 2012.
 */

public class SOSImpl {
	private static final int MAX_ITER = 100;
	private static final double TOL = 1.0e-2;
	private double perplexity;

	public SOSImpl(Params params) {
		perplexity = params.get(SosParams.PERPLEXITY);
	}

	private static DataSet <Tuple2 <Integer, DenseVector>>
	computeDissimilarityVectors(DataSet <Tuple2 <Integer, DenseVector>> inputVectors) {
		return inputVectors
			.cross(inputVectors)
			.with(
				new CrossFunction <Tuple2 <Integer, DenseVector>, Tuple2 <Integer, DenseVector>, Tuple3 <Integer,
					Integer, Double>>() {
					@Override
					public Tuple3 <Integer, Integer, Double> cross(Tuple2 <Integer, DenseVector> val1,
																   Tuple2 <Integer, DenseVector> val2)
						throws Exception {
						return Tuple3.of(val1.f0, val2.f0, MatVecOp.minus(val1.f1, val2.f1).normL2Square());
					}
				})
			.groupBy(0)
			.sortGroup(1, Order.ASCENDING)
			.reduceGroup(new GroupReduceFunction <Tuple3 <Integer, Integer, Double>, Tuple2 <Integer, DenseVector>>() {
				@Override
				public void reduce(Iterable <Tuple3 <Integer, Integer, Double>> values,
								   Collector <Tuple2 <Integer, DenseVector>> out) throws Exception {
					int id = -1;
					List <Double> d = new ArrayList <>();
					for (Tuple3 <Integer, Integer, Double> v : values) {
						id = v.f0;
						d.add(v.f2);
					}
					DenseVector denseVector = new DenseVector(d.size());
					for (int i = 0; i < d.size(); i++) {
						denseVector.set(i, d.get(i));
					}
					out.collect(Tuple2.of(id, denseVector));
				}
			})
			.name("computeDissimilarityVectors");
	}

	private static double solveForBeta(int idx, DenseVector d, double beta0, double h, int maxIter, double tol) {
		int iter = 0;
		double beta = beta0;
		double logh = Math.log(h);
		double hCurr = computeLogH(idx, d, beta);
		double err = hCurr - logh;
		double betaMin = 0.;
		double betaMax = Double.MAX_VALUE;

		while (iter < maxIter && (Double.isNaN(err) || Math.abs(err) > tol)) {
			if (Double.isNaN(err)) {
				beta = beta / 10.0;
			} else {
				if (err > 0.) { // should increase beta
					if (betaMax == Double.MAX_VALUE) {
						betaMin = beta;
						beta *= 2;
					} else {
						betaMin = beta;
						beta = 0.5 * (betaMin + betaMax);
					}
				} else { // should decrease beta
					betaMax = beta;
					beta = 0.5 * (betaMin + beta);
				}
			}
			iter++;

			hCurr = computeLogH(idx, d, beta);
			err = hCurr - logh;
		}

		return beta;
	}

	private static double computeLogH(int idx, DenseVector d, double beta) {
		double[] affinity = new double[d.size()];
		double sumA = 0.;
		for (int i = 0; i < affinity.length; i++) {
			if (i == idx) {
				affinity[i] = 0.;
				continue;
			}
			affinity[i] = Math.exp(-beta * d.get(i));
			sumA += affinity[i];
		}

		double logh = 0.;
		for (int i = 0; i < affinity.length; i++) {
			double v = affinity[i] * d.get(i);
			logh += v;
		}
		logh *= (beta / sumA);
		logh += Math.log(sumA);

		return logh;
	}

	DataSet <Tuple2 <Integer, DenseVector>>
	computeBindingProbabilities(DataSet <Tuple2 <Integer, DenseVector>> dissimilarityVectors,
								final double perplexity, final int maxIter, final double tol) {
		return dissimilarityVectors
			.map(new MapFunction <Tuple2 <Integer, DenseVector>, Tuple2 <Integer, DenseVector>>() {
				@Override
				public Tuple2 <Integer, DenseVector> map(Tuple2 <Integer, DenseVector> row) throws Exception {
					int id = row.f0;
					DenseVector dissmilarity = row.f1;

					// beta: 1 / (2 * sigma^2)
					// compute beta by solving a nonlinear equation
					double beta = solveForBeta(id, dissmilarity, 1.0, perplexity, maxIter, tol);

					// (1) the affinity that data point xi has with data point xj decays Gaussian-like with
					// respect to the dissimilarity dij
					// (2) a data point has no affinity with itself, i.e., aii = 0

					DenseVector ret = DenseVector.zeros(row.f1.size());
					double s = 0.;

					// compute the affinity
					for (int i = 0; i < dissmilarity.size(); i++) {
						if (i != id) {
							double v = dissmilarity.get(i);
							v = Math.exp(-v * beta);
							s += v;
							ret.set(i, v);
						}
					}

					// compute the binding probability
					ret.scaleEqual(1.0 / s);

					return Tuple2.of(row.f0, ret);
				}
			})
			.withForwardedFields("f0")
			.name("computeBindingProbabilities");
	}

	DataSet <Tuple2 <Integer, Double>>
	computeOutlierProbability(DataSet <Tuple2 <Integer, DenseVector>> bindingProbabilityVectors) {
		return bindingProbabilityVectors
			. <Tuple2 <Integer, Double>>flatMap(
				new FlatMapFunction <Tuple2 <Integer, DenseVector>, Tuple2 <Integer, Double>>() {
					@Override
					public void flatMap(Tuple2 <Integer, DenseVector> in,
										Collector <Tuple2 <Integer, Double>> collector) throws Exception {
						for (int i = 0; i < in.f1.size(); i++) {
							double v = in.f1.get(i);
							collector.collect(new Tuple2 <>(i, v));
						}
					}
				})
			.groupBy(0)
			. <Tuple2 <Integer, Double>>reduceGroup(
				new GroupReduceFunction <Tuple2 <Integer, Double>, Tuple2 <Integer, Double>>() {
					@Override
					public void reduce(Iterable <Tuple2 <Integer, Double>> iterable,
									   Collector <Tuple2 <Integer, Double>> collector) throws Exception {
						double s = 1.0;
						int nodeId = -1;
						for (Tuple2 <Integer, Double> bindingProb : iterable) {
							nodeId = bindingProb.f0;
							s *= (1.0 - bindingProb.f1);
						}
						collector.collect(new Tuple2 <>(nodeId, s));
					}
				});
	}

	/**
	 * compute outlier probabilities for each sample
	 */
	public DataSet <Tuple2 <Integer, Double>> outlierSelection(DataSet <Tuple2 <Integer, DenseVector>> inputVectors) {

		DataSet <Tuple2 <Integer, DenseVector>> dissimilarityVectors = computeDissimilarityVectors(inputVectors);
		DataSet <Tuple2 <Integer, DenseVector>> bindingProbabilityVectors =
			computeBindingProbabilities(dissimilarityVectors, perplexity, MAX_ITER, TOL);
		return computeOutlierProbability(bindingProbabilityVectors);
	}
}
