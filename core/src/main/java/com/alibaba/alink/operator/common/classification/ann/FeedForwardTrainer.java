package com.alibaba.alink.operator.common.classification.ann;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkIllegalModelException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.optim.Lbfgs;
import com.alibaba.alink.operator.common.optim.Optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * The trainer for feed forward neural networks.
 */
public class FeedForwardTrainer implements Serializable {
	private static final long serialVersionUID = -1664569158355844836L;
	private final Topology topology;
	private final int inputSize;
	private final int outputSize;
	private final int blockSize;
	private final boolean onehotLabel;
	private final DenseVector initialWeights;

	/**
	 * The Constructor.
	 *
	 * @param topology       The network topology.
	 * @param inputSize      Size of input.
	 * @param outputSize     Size of output.
	 * @param onehotLabel    Whether to onehot the label.
	 * @param blockSize      Stack size.
	 * @param initialWeights Initial weights of the network.
	 */
	public FeedForwardTrainer(Topology topology, int inputSize, int outputSize,
							  boolean onehotLabel, int blockSize, DenseVector initialWeights) {
		this.topology = topology;
		this.inputSize = inputSize;
		this.outputSize = outputSize;
		this.onehotLabel = onehotLabel;
		this.blockSize = blockSize;
		this.initialWeights = initialWeights;
	}

	/**
	 * Train the network.
	 *
	 * @param data               Training data, a dataset of tuples of (label, features).
	 * @param optimizationParams Parameters for optimizations.
	 * @return The model weights.
	 */
	public DataSet <DenseVector> train(DataSet <Tuple2 <Double, DenseVector>> data, DataSet<DenseVector> initialModel,
									   Params optimizationParams) {
		final Topology topology = this.topology;
		final int inputSize = this.inputSize;
		final int outputSize = this.outputSize;
		final boolean onehotLabel = this.onehotLabel;

		ParamInfo <Integer> NUM_SEARCH_STEP = ParamInfoFactory
			.createParamInfo("numSearchStep", Integer.class)
			.setDescription("num search step")
			.setRequired()
			.build();

		DataSet <DenseVector> initCoef = initialModel != null ? initialModel : initModel(data, this.topology);
		DataSet <Tuple3 <Double, Double, Vector>> trainData = stack(data, blockSize, inputSize, outputSize,
			onehotLabel);
		optimizationParams.set(NUM_SEARCH_STEP, 3);
		final AnnObjFunc annObjFunc = new AnnObjFunc(topology, inputSize, outputSize, onehotLabel, optimizationParams);

		// We always use L-BFGS to train the network.
		Optimizer optimizer = new Lbfgs(
			data.getExecutionEnvironment().fromElements(annObjFunc),
			trainData,
			BatchOperator
				.getExecutionEnvironmentFromDataSets(data)
				.fromElements(inputSize),
			optimizationParams
		);
		optimizer.initCoefWith(initCoef);
		return optimizer.optimize().map(new MapFunction <Tuple2 <DenseVector, double[]>, DenseVector>() {
			private static final long serialVersionUID = -6247802998516251320L;

			@Override
			public DenseVector map(Tuple2 <DenseVector, double[]> value) {
				return value.f0;
			}
		});
	}

	private static DataSet <Tuple3 <Double, Double, Vector>>
	stack(DataSet <Tuple2 <Double, DenseVector>> data, final int batchSize,
		  final int inputSize, final int outputSize, final boolean onehot) {
		return data
			.mapPartition(new MapPartitionFunction <Tuple2 <Double, DenseVector>, Tuple3 <Double, Double, Vector>>() {
				private static final long serialVersionUID = -4065550804759190453L;

				@Override
				public void mapPartition(Iterable <Tuple2 <Double, DenseVector>> samples,
										 Collector <Tuple3 <Double, Double, Vector>> out) {
					List <Tuple2 <Double, DenseVector>> batchData = new ArrayList <>(batchSize);
					for (int i = 0; i < batchSize; i++) {
						batchData.add(null);
					}

					int cnt = 0;
					Stacker stacker = new Stacker(inputSize, outputSize, onehot);
					for (Tuple2 <Double, DenseVector> sample : samples) {
						batchData.set(cnt, sample);
						cnt++;
						if (cnt >= batchSize) {
							Tuple3 <Double, Double, Vector> batch = stacker.stack(batchData, cnt);
							out.collect(batch);
							cnt = 0;
						}
					}

					if (cnt > 0) {
						Tuple3 <Double, Double, Vector> batch = stacker.stack(batchData, cnt);
						out.collect(batch);
					}
				}
			})
			.name("stack_data");
	}

	private DataSet <DenseVector> initModel(DataSet <?> inputRel, final Topology topology) {
		if (initialWeights != null) {
			if (initialWeights.size() != topology.getWeightSize()) {
				throw new AkIllegalModelException("Invalid initial weights, size mismatch");
			}
			return BatchOperator.getExecutionEnvironmentFromDataSets(inputRel).fromElements(this.initialWeights);
		} else {
			return BatchOperator.getExecutionEnvironmentFromDataSets(inputRel).fromElements(0)
				.map(new RichMapFunction <Integer, DenseVector>() {
					private static final long serialVersionUID = 8668081633098768854L;
					final double initStdev = 0.05;
					final long seed = 1L;
					transient Random random;

					@Override
					public void open(Configuration parameters) {
						random = new Random(seed);
					}

					@Override
					public DenseVector map(Integer value) {
						DenseVector weights = DenseVector.zeros(topology.getWeightSize());
						for (int i = 0; i < weights.size(); i++) {
							weights.set(i, random.nextGaussian() * initStdev);
						}
						return weights;
					}
				})
				.name("init_weights");
		}
	}
}