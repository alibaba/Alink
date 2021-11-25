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
import com.alibaba.alink.testutil.AlinkTestBase;
import com.alibaba.alink.testutil.categories.ICQTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for IterativeComQueue.
 */

public class IterativeComQueueTest extends AlinkTestBase implements Serializable {

	private static final String TRAIN_DATA = "trainData";
	private static final String COEFS = "coefs";
	private static final String SAMPLE_COUNT = "sampleCount";
	private static final String COEFS_ARRAY = "coefsArray";
	private static final long serialVersionUID = -993578804337371556L;

	@Category(ICQTest.class)
	@Test
	public void testPI() throws Exception {
		IterativeComQueue iterativeComQueue = new IterativeComQueue();

		DataSet <Row> result = iterativeComQueue
			.add(new ComputeFunction() {
				private static final long serialVersionUID = -8276011238250174901L;

				@Override
				public void calc(ComContext context) {
					if (1 == context.getStepNo()) {
						context.putObj("cnt", new int[] {0});
					}
					int[] cnt = context.getObj("cnt");
					double x = Math.random();
					double y = Math.random();
					cnt[0] += ((x * x + y * y < 1) ? 1 : 0);
				}
			})
			.closeWith(new CompleteResultFunction() {
				private static final long serialVersionUID = -1334011112883033932L;

				@Override
				public List <Row> calc(ComContext context) {
					int[] cnt = context.getObj("cnt");
					return Collections.singletonList(Row.of(4.0 * cnt[0] / 30));
				}
			})
			.setMaxIter(30)
			.exec();

		Assert.assertEquals(3.1, (double) result.collect().get(0).getField(0), 1.0);

		iterativeComQueue.getQueue().clear();

		iterativeComQueue
			.add(new ComputeFunction() {
				private static final long serialVersionUID = -144300478371050867L;

				@Override
				public void calc(ComContext context) {
					Assert.assertNull(context.getObj("cnt"));
				}
			})
			.closeWith(new CompleteResultFunction() {
				private static final long serialVersionUID = 291263711007021018L;

				@Override
				public List <Row> calc(ComContext context) {
					Assert.assertNull(context.getObj("cnt"));
					return null;
				}
			})
			.exec()
			.collect();
	}

	@Category(ICQTest.class)
	@Test
	public void testICQLinearRegression() throws Exception {
		final int m = 1000;
		final int n = 3;

		List <Tuple2 <DenseVector, Double>> data = new ArrayList <>();

		for (int i = 0; i < m; ++i) {
			DenseVector feature = DenseVector.rand(n);
			data.add(Tuple2.of(feature.append(1.0), feature.dot(DenseVector.ones(n))));
		}

		DataSet <Tuple2 <DenseVector, Double>> trainData =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(data);

		DataSet <DenseVector> initialCoefs =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(Collections.singletonList(DenseVector.zeros(n + 1)));

		DataSet <Double> sampleCount = DataSetUtils
			.countElementsPerPartition(trainData)
			.sum(1)
			.map(new MapFunction <Tuple2 <Integer, Long>, Double>() {
				private static final long serialVersionUID = 3361803167964030022L;

				@Override
				public Double map(Tuple2 <Integer, Long> value) throws Exception {
					return value.f1.doubleValue();
				}
			});

		final double learningRate = 1.0;

		DataSet <Row> model = new IterativeComQueue()
			.setMaxIter(100)
			.initWithPartitionedData(TRAIN_DATA, trainData)
			.initWithBroadcastData(COEFS, initialCoefs)
			.initWithBroadcastData(SAMPLE_COUNT, sampleCount)
			.add(new ComputeFunction() {
				private static final long serialVersionUID = -7341603684624763260L;

				@Override
				public void calc(ComContext context) {
					List <Tuple2 <DenseVector, Double>> trainData = context.getObj(TRAIN_DATA);
					List <DenseVector> coefs = context.getObj(COEFS);
					double[] grads = context.getObj("grads");

					if (grads == null) {
						grads = new double[coefs.get(0).size()];
						context.putObj("grads", grads);
					}

					Arrays.fill(grads, 0.0);

					DenseVector gradsWrapper = new DenseVector(grads);

					for (Tuple2 <DenseVector, Double> sample : trainData) {
						gradsWrapper.plusScaleEqual(sample.f0, sample.f1 - sample.f0.dot(coefs.get(0)));
					}
				}
			})
			.add(new AllReduce("grads"))
			.add(new ComputeFunction() {
				private static final long serialVersionUID = 7104317989801807311L;

				@Override
				public void calc(ComContext context) {
					List <DenseVector> coefs = context.getObj(COEFS);
					double[] grads = context.getObj("grads");
					List <Double> sampleCount = context.getObj(SAMPLE_COUNT);

					coefs.get(0).plusScaleEqual(new DenseVector(grads), learningRate / sampleCount.get(0));
				}
			})
			.closeWith(new CompleteResultFunction() {
				private static final long serialVersionUID = -1786146665537149148L;

				@Override
				public List <Row> calc(ComContext context) {
					if (context.getTaskId() == 0) {
						List <DenseVector> coefs = context.getObj(COEFS);
						return Collections.singletonList(Row.of(coefs.get(0)));
					} else {
						return null;
					}
				}
			})
			.exec();

		List <Row> modelL = model.collect();

		Assert.assertEquals(data.get(0).f1,
			data.get(0).f0.dot((Vector) modelL.get(0).getField(0)),
			2.0
		);
	}

	@Test
	public void testICQLinearRegression1() throws Exception {
		final int m = 1000;
		final int n = 20;

		List <Tuple2 <DenseVector, Double>> data = new ArrayList <>();

		for (int i = 0; i < m; ++i) {
			DenseVector feature = DenseVector.rand(n);
			data.add(Tuple2.of(feature.append(1.0), feature.dot(DenseVector.ones(n))));
		}

		DataSet <Tuple2 <DenseVector, Double>> trainData =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(data)
				.rebalance();

		DataSet <DenseVector> initialCoefs =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(Collections.singletonList(DenseVector.zeros(n + 1)));

		DataSet <Double> sampleCount = DataSetUtils
			.countElementsPerPartition(trainData)
			.sum(1)
			.map(new MapFunction <Tuple2 <Integer, Long>, Double>() {
				private static final long serialVersionUID = 4461084761046487279L;

				@Override
				public Double map(Tuple2 <Integer, Long> value) throws Exception {
					return value.f1.doubleValue();
				}
			});

		final double learningRate = 1.0;
		final double decayLrRate = 0.8;
		final double decayStep = 5;

		DataSet <Row> model = new IterativeComQueue()
			.setMaxIter(1000)
			.initWithPartitionedData(TRAIN_DATA, trainData)
			.initWithBroadcastData(COEFS, initialCoefs)
			.initWithBroadcastData(SAMPLE_COUNT, sampleCount)
			.add(new UpdateCoefs(learningRate, decayStep, decayLrRate))
			.add(new AllReduce(COEFS_ARRAY))
			.closeWith(new SerializeModel())
			.exec();

		List <Row> modelL = model.collect();

		Assert.assertEquals(data.get(0).f1,
			data.get(0).f0.dot((Vector) modelL.get(0).getField(0)),
			2.0
		);
	}

	@Test
	public void testFlinkLinerRegression() throws Exception {
		final int m = 1000;
		final int n = 1;

		List <Tuple2 <DenseVector, Double>> data = new ArrayList <>();

		for (int i = 0; i < m; ++i) {
			DenseVector feature = DenseVector.rand(n);
			data.add(Tuple2.of(feature.append(1.0), feature.dot(DenseVector.ones(n))));
		}

		DataSet <Tuple2 <DenseVector, Double>> trainData =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(data)
				.rebalance();

		final double learningRate = 1.0;

		DataSet <Tuple2 <DenseVector, Double>> initialCoefs =
			MLEnvironmentFactory
				.getDefault()
				.getExecutionEnvironment()
				.fromCollection(Collections.singletonList(Tuple2.of(DenseVector.zeros(n + 1), learningRate)));

		DataSet <Double> sampleCount = DataSetUtils
			.countElementsPerPartition(trainData)
			.sum(1)
			.map(new MapFunction <Tuple2 <Integer, Long>, Double>() {
				private static final long serialVersionUID = -3266186428603129419L;

				@Override
				public Double map(Tuple2 <Integer, Long> value) throws Exception {
					return value.f1.doubleValue();
				}
			});

		final double decayLrRate = 1.0;
		final double decayStep = 5;

		IterativeDataSet <Tuple2 <DenseVector, Double>> loop = initialCoefs.iterate(1000);

		DataSet <Tuple2 <DenseVector, Double>> newParameters = trainData
			.map(new SubUpdate(decayStep, decayLrRate))
			.withBroadcastSet(loop, COEFS)
			.reduce(new UpdateAccumulator())
			.map(new Update())
			.withBroadcastSet(sampleCount, SAMPLE_COUNT);

		DataSet <Tuple2 <DenseVector, Double>> model = loop.closeWith(newParameters);

		List <Tuple2 <DenseVector, Double>> modelL = model.collect();

		Assert.assertEquals(data.get(0).f1,
			data.get(0).f0.dot(modelL.get(0).getField(0)),
			2.0
		);
	}

	@Category(ICQTest.class)
	@Test
	public void testICQ() {
		try {
			DataSet <Row> ret = new IterativeComQueue()
				.setMaxIter(1)
				.add(new ComputeFunction() {
					private static final long serialVersionUID = -6857850489553631270L;

					@Override
					public void calc(ComContext context) {
						context.putObj("allReduce", new double[] {1.0, 1.0, 1.0});
					}
				})
				.add(new AllReduce("allReduce"))
				.add(new ComputeFunction() {
					private static final long serialVersionUID = -4755589100955942726L;

					@Override
					public void calc(ComContext context) {
						double[] result = context.getObj("allReduce");

						System.out.println(JsonConverter.toJson(result));
					}
				})
				.closeWith(new CompleteResultFunction() {
					private static final long serialVersionUID = 736055612455949813L;

					@Override
					public List <Row> calc(ComContext context) {
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
		private static final long serialVersionUID = -4751805122244715217L;
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
			List <Tuple2 <DenseVector, Double>> trainData = context.getObj(TRAIN_DATA);
			List <DenseVector> coefs = context.getObj(COEFS);
			List <Double> sampleCount = context.getObj(SAMPLE_COUNT);
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
			for (Tuple2 <DenseVector, Double> sample : trainData) {
				DenseVector coefsTmp = coefs0.clone();
				coefsTmp.plusScaleEqual(sample.f0, decayedLr * (sample.f1 - sample.f0.dot(coefsTmp)));
				coefs.get(0).plusEqual(coefsTmp);
			}
		}
	}

	private static class SerializeModel extends CompleteResultFunction {
		private static final long serialVersionUID = 2711192194365351526L;

		@Override
		public List <Row> calc(ComContext context) {
			if (context.getTaskId() == 0) {
				List <DenseVector> coefs = context.getObj(COEFS);
				List <Double> sampleCount = context.getObj(SAMPLE_COUNT);
				coefs.get(0).scaleEqual(1.0 / sampleCount.get(0));
				return Collections.singletonList(Row.of(coefs.get(0)));
			} else {
				return null;
			}
		}
	}

	private static class SubUpdate
		extends RichMapFunction <Tuple2 <DenseVector, Double>, Tuple2 <DenseVector, Double>> {
		private static final long serialVersionUID = 7415239235952062602L;
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
			Collection <Tuple2 <DenseVector, Double>> coefsList = getRuntimeContext().getBroadcastVariable(COEFS);
			for (Tuple2 <DenseVector, Double> c : coefsList) {
				coefs = c.f0;
				lr = c.f1;
			}

			if (getIterationRuntimeContext().getSuperstepNumber() % decayStep == 0) {
				lr = lr * decayLrRate;
			}
		}

		@Override
		public Tuple2 <DenseVector, Double> map(Tuple2 <DenseVector, Double> value) throws Exception {
			DenseVector newCoefs = coefs.clone();
			newCoefs.plusScaleEqual(value.f0, lr * (value.f1 - value.f0.dot(coefs)));
			return Tuple2.of(newCoefs, lr);
		}
	}

	private static class UpdateAccumulator implements ReduceFunction <Tuple2 <DenseVector, Double>> {
		private static final long serialVersionUID = 456315243568557623L;

		@Override
		public Tuple2 <DenseVector, Double> reduce(Tuple2 <DenseVector, Double> value1,
												   Tuple2 <DenseVector, Double> value2) throws Exception {
			value1.f0.plusEqual(value2.f0);
			return Tuple2.of(value1.f0, value1.f1);
		}
	}

	private static class Update extends RichMapFunction <Tuple2 <DenseVector, Double>, Tuple2 <DenseVector, Double>> {
		private static final long serialVersionUID = -2970335698410933630L;
		double sampleCount;

		@Override
		public void open(Configuration parameters) throws Exception {
			Collection <Double> sampleCountList = getRuntimeContext().getBroadcastVariable(SAMPLE_COUNT);

			for (Double d : sampleCountList) {
				sampleCount = d;
			}
		}

		@Override
		public Tuple2 <DenseVector, Double> map(Tuple2 <DenseVector, Double> value) throws Exception {
			return Tuple2.of(value.f0.scale(1.0 / sampleCount), value.f1);
		}
	}
}