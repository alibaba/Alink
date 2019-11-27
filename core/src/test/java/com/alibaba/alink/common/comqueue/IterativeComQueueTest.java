package com.alibaba.alink.common.comqueue;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.JsonConverter;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for IterativeComQueue.
 */
public class IterativeComQueueTest implements Serializable {

	private static final String TRAIN_DATA = "trainData";
	private static final String COEFS = "coefs";
	private static final String SAMPLE_COUNT = "sampleCount";
	private static final String COEFS_ARRAY = "coefsArray";

	@Test
	public void testPI() throws Exception {
		MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(1);

		DataSet<Row> result = new IterativeComQueue()
			.add(new ComputeFunction() {
				@Override
				public void calc(ComContext context) {
					if (1 == context.getStepNo()) {
						context.putObj("cnt", new int[]{0});
					}
					int[] cnt = context.getObj("cnt");
					double x = Math.random();
					double y = Math.random();
					cnt[0] += ((x * x + y * y < 1) ? 1 : 0);
				}
			})
			.closeWith(new CompleteResultFunction() {
				@Override
				public List<Row> calc(ComContext context) {
					int[] cnt = context.getObj("cnt");
					return Collections.singletonList(Row.of(4.0 * cnt[0] / 1000));
				}
			})
			.setMaxIter(1000)
			.exec();

		Assert.assertEquals(3.0, (double) result.collect().get(0).getField(0), 0.5);
	}

	@Test
	public void testICQLinearRegression() throws Exception {
		final int m = 1000000;
		final int n = 3;

		List<Tuple2<DenseVector, Double>> data = new ArrayList<>();

		for (int i = 0; i < m; ++i) {
			DenseVector feature = DenseVector.rand(n);
			data.add(Tuple2.of(feature.append(1.0), feature.dot(DenseVector.ones(n))));
		}

		DataSet<Tuple2<DenseVector, Double>> trainData =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(data);

		DataSet<DenseVector> initialCoefs =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(Collections.singletonList(DenseVector.zeros(n + 1)));

		DataSet<Double> sampleCount = DataSetUtils
			.countElementsPerPartition(trainData)
			.sum(1)
			.map(new MapFunction<Tuple2<Integer, Long>, Double>() {
				@Override
				public Double map(Tuple2<Integer, Long> value) throws Exception {
					return value.f1.doubleValue();
				}
			});

		final double learningRate = 1.0;

		DataSet<Row> model = new IterativeComQueue()
			.setMaxIter(100)
			.initWithPartitionedData(TRAIN_DATA, trainData)
			.initWithBroadcastData(COEFS, initialCoefs)
			.initWithBroadcastData(SAMPLE_COUNT, sampleCount)
			.add(new ComputeFunction() {
				@Override
				public void calc(ComContext context) {
					List<Tuple2<DenseVector, Double>> trainData = context.getObj(TRAIN_DATA);
					List<DenseVector> coefs = context.getObj(COEFS);
					double[] grads = context.getObj("grads");

					if (grads == null) {
						grads = new double[coefs.get(0).size()];
						context.putObj("grads", grads);
					}

					Arrays.fill(grads, 0.0);

					DenseVector gradsWrapper = new DenseVector(grads);

					for (Tuple2<DenseVector, Double> sample : trainData) {
						gradsWrapper.plusScaleEqual(sample.f0, sample.f1 - sample.f0.dot(coefs.get(0)));
					}
				}
			})
			.add(new AllReduce("grads"))
			.add(new ComputeFunction() {
				@Override
				public void calc(ComContext context) {
					List<DenseVector> coefs = context.getObj(COEFS);
					double[] grads = context.getObj("grads");
					List<Double> sampleCount = context.getObj(SAMPLE_COUNT);

					coefs.get(0).plusScaleEqual(new DenseVector(grads), learningRate / sampleCount.get(0));
				}
			})
			.closeWith(new CompleteResultFunction() {
				@Override
				public List<Row> calc(ComContext context) {
					if (context.getTaskId() == 0) {
						List<DenseVector> coefs = context.getObj(COEFS);
						return Collections.singletonList(Row.of(coefs.get(0)));
					} else {
						return null;
					}
				}
			})
			.exec();

		List<Row> modelL = model.collect();

		Assert.assertEquals(data.get(0).f1,
			data.get(0).f0.dot((Vector) modelL.get(0).getField(0)),
			2.0
		);
	}

	@Test
	public void testICQLinearRegression1() throws Exception {
		final long start = System.currentTimeMillis();
		final int m = 1000000;
		final int n = 20;

		List<Tuple2<DenseVector, Double>> data = new ArrayList<>();

		for (int i = 0; i < m; ++i) {
			DenseVector feature = DenseVector.rand(n);
			data.add(Tuple2.of(feature.append(1.0), feature.dot(DenseVector.ones(n))));
		}

		DataSet<Tuple2<DenseVector, Double>> trainData =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(data)
				.rebalance();

		DataSet<DenseVector> initialCoefs =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(Collections.singletonList(DenseVector.zeros(n + 1)));

		DataSet<Double> sampleCount = DataSetUtils
			.countElementsPerPartition(trainData)
			.sum(1)
			.map(new MapFunction<Tuple2<Integer, Long>, Double>() {
				@Override
				public Double map(Tuple2<Integer, Long> value) throws Exception {
					return value.f1.doubleValue();
				}
			});

		final double learningRate = 1.0;
		final double decayLrRate = 0.8;
		final double decayStep = 5;

		DataSet<Row> model = new IterativeComQueue()
			.setMaxIter(1000)
			.initWithPartitionedData(TRAIN_DATA, trainData)
			.initWithBroadcastData(COEFS, initialCoefs)
			.initWithBroadcastData(SAMPLE_COUNT, sampleCount)
			.add(new UpdateCoefs(learningRate, decayStep, decayLrRate))
			.add(new AllReduce(COEFS_ARRAY))
			.closeWith(new SerializeModel())
			.exec();

		List<Row> modelL = model.collect();

		System.out.println(JsonConverter.toJson(modelL));
		for (int i = 0; i < 10; ++i) {
			System.out.println(
				JsonConverter.toJson(
					Tuple2.of(
						data.get(i).f1,
						data.get(i).f0.dot((Vector) modelL.get(0).getField(0))
					)
				)
			);
		}

		final long end = System.currentTimeMillis();

		System.out.println(String.format("ICQ time: %d", end - start));

		Assert.assertEquals(data.get(0).f1,
			data.get(0).f0.dot((Vector) modelL.get(0).getField(0)),
			2.0
		);
	}

	@Test
	public void testFlinkLinerRegression() throws Exception {
		final int m = 1000000;
		final int n = 1;

		List<Tuple2<DenseVector, Double>> data = new ArrayList<>();

		for (int i = 0; i < m; ++i) {
			DenseVector feature = DenseVector.rand(n);
			data.add(Tuple2.of(feature.append(1.0), feature.dot(DenseVector.ones(n))));
		}

		final long start = System.currentTimeMillis();
		DataSet<Tuple2<DenseVector, Double>> trainData =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(data)
				.rebalance();

		final double learningRate = 1.0;

		DataSet<Tuple2<DenseVector, Double>> initialCoefs =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(Collections.singletonList(Tuple2.of(DenseVector.zeros(n + 1), learningRate)));

		DataSet<Double> sampleCount = DataSetUtils
			.countElementsPerPartition(trainData)
			.sum(1)
			.map(new MapFunction<Tuple2<Integer, Long>, Double>() {
				@Override
				public Double map(Tuple2<Integer, Long> value) throws Exception {
					return value.f1.doubleValue();
				}
			});

		final double decayLrRate = 1.0;
		final double decayStep = 5;

		IterativeDataSet<Tuple2<DenseVector, Double>> loop = initialCoefs.iterate(1000);

		DataSet<Tuple2<DenseVector, Double>> newParameters = trainData
			.map(new SubUpdate(decayStep, decayLrRate))
			.withBroadcastSet(loop, COEFS)
			.reduce(new UpdateAccumulator())
			.map(new Update())
			.withBroadcastSet(sampleCount, SAMPLE_COUNT);

		DataSet<Tuple2<DenseVector, Double>> model = loop.closeWith(newParameters);

		List<Tuple2<DenseVector, Double>> modelL = model.collect();

		final long end = System.currentTimeMillis();

		System.out.println(String.format("flink time: %d", end - start));

		System.out.println(JsonConverter.toJson(modelL));
		for (int i = 0; i < 10; ++i) {
			System.out.println(
				JsonConverter.toJson(
					Tuple2.of(
						data.get(i).f1,
						data.get(i).f0.dot(modelL.get(0).getField(0))
					)
				)
			);
		}

		Assert.assertEquals(data.get(0).f1,
			data.get(0).f0.dot(modelL.get(0).getField(0)),
			2.0
		);
	}

	@Test
	public void testICQ() {
		try {
			DataSet<Row> ret = new IterativeComQueue()
				.setMaxIter(1)
				.add(new ComputeFunction() {
					@Override
					public void calc(ComContext context) {
						context.putObj("allReduce", new double[]{1.0, 1.0, 1.0});
					}
				})
				.add(new AllReduce("allReduce"))
				.add(new ComputeFunction() {
					@Override
					public void calc(ComContext context) {
						double[] result = context.getObj("allReduce");

						System.out.println(JsonConverter.toJson(result));
					}
				})
				.closeWith(new CompleteResultFunction() {
					@Override
					public List<Row> calc(ComContext context) {
						return null;
					}
				})
				.exec();
			ret.collect();
		} catch (Exception e) {
			Assert.fail("should not throw exception");
		}
	}

	private static class UpdateCoefs extends ComputeFunction {
		private final double learningRate;
		private final double decayStep;
		private final double decayLrRate;

		public UpdateCoefs(double learningRate, double decayStep, double decayLrRate) {
			this.learningRate = learningRate;
			this.decayStep = decayStep;
			this.decayLrRate = decayLrRate;
		}

		@Override
		public void calc(ComContext context) {
			List<Tuple2<DenseVector, Double>> trainData = context.getObj(TRAIN_DATA);
			List<DenseVector> coefs = context.getObj(COEFS);
			List<Double> sampleCount = context.getObj(SAMPLE_COUNT);
			double decayedLr;
			if (context.getStepNo() == 1) {
				decayedLr = learningRate;
			} else if (context.getStepNo() % decayStep == 0) {
				decayedLr = (double) context.getObj("lr") * decayLrRate;
			} else {
				decayedLr = context.getObj("lr");
			}

			context.putObj("lr", decayedLr);

			context.putObj(COEFS_ARRAY, coefs.get(0).getData());
			DenseVector coefs0 = coefs.get(0).scale(1.0 / sampleCount.get(0));
			coefs.get(0).scaleEqual(0.0);
			for (Tuple2<DenseVector, Double> sample : trainData) {
				DenseVector coefsTmp = coefs0.clone();
				coefsTmp.plusScaleEqual(sample.f0, decayedLr * (sample.f1 - sample.f0.dot(coefsTmp)));
				coefs.get(0).plusEqual(coefsTmp);
			}
		}
	}

	private static class SerializeModel extends CompleteResultFunction {
		@Override
		public List<Row> calc(ComContext context) {
			if (context.getTaskId() == 0) {
				List<DenseVector> coefs = context.getObj(COEFS);
				List<Double> sampleCount = context.getObj(SAMPLE_COUNT);
				coefs.get(0).scaleEqual(1.0 / sampleCount.get(0));
				return Collections.singletonList(Row.of(coefs.get(0)));
			} else {
				return null;
			}
		}
	}

	private static class SubUpdate extends RichMapFunction<Tuple2<DenseVector, Double>, Tuple2<DenseVector, Double>> {
		private final double decayStep;
		private final double decayLrRate;
		DenseVector coefs;
		double lr;

		public SubUpdate(double decayStep, double decayLrRate) {
			this.decayStep = decayStep;
			this.decayLrRate = decayLrRate;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<Tuple2<DenseVector, Double>> coefsList = getRuntimeContext().getBroadcastVariable(COEFS);
			for (Tuple2<DenseVector, Double> c : coefsList) {
				coefs = c.f0;
				lr = c.f1;
			}

			if (getIterationRuntimeContext().getSuperstepNumber() % decayStep == 0) {
				lr = lr * decayLrRate;
			}
		}

		@Override
		public Tuple2<DenseVector, Double> map(Tuple2<DenseVector, Double> value) throws Exception {
			DenseVector newCoefs = coefs.clone();
			newCoefs.plusScaleEqual(value.f0, lr * (value.f1 - value.f0.dot(coefs)));
			return Tuple2.of(newCoefs, lr);
		}
	}

	private static class UpdateAccumulator implements ReduceFunction<Tuple2<DenseVector, Double>> {
		@Override
		public Tuple2<DenseVector, Double> reduce(Tuple2<DenseVector, Double> value1, Tuple2<DenseVector, Double> value2) throws Exception {
			value1.f0.plusEqual(value2.f0);
			return Tuple2.of(value1.f0, value1.f1);
		}
	}

	private static class Update extends RichMapFunction<Tuple2<DenseVector, Double>, Tuple2<DenseVector, Double>> {
		double sampleCount;

		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<Double> sampleCountList = getRuntimeContext().getBroadcastVariable(SAMPLE_COUNT);

			for (Double d : sampleCountList) {
				sampleCount = d;
			}
		}

		@Override
		public Tuple2<DenseVector, Double> map(Tuple2<DenseVector, Double> value) throws Exception {
			return Tuple2.of(value.f0.scale(1.0 / sampleCount), value.f1);
		}
	}
}