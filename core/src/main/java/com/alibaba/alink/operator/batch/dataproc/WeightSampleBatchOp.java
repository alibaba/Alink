package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.WeightSampleParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
/**
 * Weighted sampling with given ratio with/without replacement.
 * The probability of being chosen for data point {i} is: weight_{i} / \sum_{j=1}^{n} weight_{j}, where weight_{i} is
 * the weight of data point i.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@ParamSelectColumnSpec(name = "weightCol", allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@NameCn("加权采样")
public class WeightSampleBatchOp extends BatchOperator <WeightSampleBatchOp>
	implements WeightSampleParams <WeightSampleBatchOp> {

	private static final long serialVersionUID = 8815784097940967758L;
	private static String COUNT = "count";
	private static String BOUNDS = "bounds";

	public WeightSampleBatchOp() {
		this(null);
	}

	public WeightSampleBatchOp(Params params) {
		super(params);

	}

	@Override
	public WeightSampleBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator in = checkAndGetFirst(inputs);
		DataSet <Row> data = in.getDataSet();

		int weightIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), getWeightCol());
		double ratio = getRatio();

		if (getWithReplacement()) {
			//TaskId, count, weightSum
			DataSet <Tuple3 <Integer, Integer, Double>> weight = data.mapPartition(
				new RichMapPartitionFunction <Row, Tuple3 <Integer, Integer, Double>>() {
					private static final long serialVersionUID = -684553157530047702L;

					@Override
					public void mapPartition(Iterable <Row> values, Collector <Tuple3 <Integer, Integer, Double>> out) {
						int cnt = 0;
						double sum = 0.;
						int taskId = getRuntimeContext().getIndexOfThisSubtask();
						for (Row row : values) {
							double weight = ((Number) row.getField(weightIdx)).doubleValue();
							AkPreconditions.checkArgument(weight > 0 && !Double.isNaN(weight) && Double.isFinite(weight),
								new AkIllegalDataException("Weight must be positive!"));
							cnt++;
							sum += weight;
						}
						out.collect(Tuple3.of(taskId, cnt, sum));
					}
				});

			DataSet <Tuple2 <Integer, double[]>> bounds = weight.reduceGroup(
				new GroupReduceFunction <Tuple3 <Integer, Integer, Double>, Tuple2 <Integer, double[]>>() {
					private static final long serialVersionUID = 2912858605429940900L;

					@Override
					public void reduce(Iterable <Tuple3 <Integer, Integer, Double>> values,
									   Collector <Tuple2 <Integer, double[]>> out) {
						List <Tuple3 <Integer, Integer, Double>> list = new ArrayList <>();
						values.forEach(list::add);
						Collections.sort(list, Comparator.comparingDouble(t -> t.f0));
						double[] bounds = new double[list.size() + 1];
						int cnt = 0;
						for (int i = 0; i < list.size(); i++) {
							bounds[i + 1] = bounds[i] + list.get(i).f2;
							cnt += list.get(i).f1;
						}
						out.collect(Tuple2.of(cnt, bounds));
					}
				});

			final Random random = new Random(0);
			DataSet <Row> res = data
				.mapPartition(new RandomSelect(random, ratio, weightIdx))
				.withBroadcastSet(bounds, BOUNDS);

			this.setOutput(res, in.getSchema());
		} else {
			DataSet <Tuple2 <Double, Row>> weight = data.mapPartition(
				new RichMapPartitionFunction <Row, Tuple2 <Double, Row>>() {
					private static final long serialVersionUID = -9150449993114999173L;

					@Override
					public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Double, Row>> out) {
						Random random = new Random(getRuntimeContext().getIndexOfThisSubtask());
						for (Row row : values) {
							double weight = ((Number) row.getField(weightIdx)).doubleValue();
							AkPreconditions.checkArgument(weight > 0 && !Double.isNaN(weight) && Double.isFinite(weight),
								new AkIllegalDataException("Weight must be positive!"));
							double rp = random.nextDouble();
							while (rp <= 1e-30) {
								rp = random.nextDouble();
							}
							out.collect(Tuple2.of(Math.log(rp) / weight, row));
						}
					}
				}).partitionByRange(0);

			//taskId, count, minValue
			DataSet <Tuple3 <Integer, Integer, Double>> taskCnts = weight.mapPartition(
				new RichMapPartitionFunction <Tuple2 <Double, Row>, Tuple3 <Integer, Integer, Double>>() {
					private static final long serialVersionUID = -281138469922874075L;

					@Override
					public void mapPartition(Iterable <Tuple2 <Double, Row>> values,
											 Collector <Tuple3 <Integer, Integer, Double>> out) {
						int taskId = getRuntimeContext().getIndexOfThisSubtask();
						int cnt = 0;
						double min = Double.MAX_VALUE;
						for (Tuple2 <Double, Row> t : values) {
							min = Math.min(t.f0, min);
							cnt++;
						}
						out.collect(Tuple3.of(taskId, cnt, min));
					}
				});

			DataSet <Row> res = weight.mapPartition(new TopNSelect(ratio))
				.withBroadcastSet(taskCnts, COUNT);

			this.setOutput(res, in.getSchema());
		}
		return this;
	}

	private static class RandomSelect extends RichMapPartitionFunction <Row, Row> {
		private static final long serialVersionUID = 5592394863599823024L;
		private Random random;
		private List <Double> cuts;
		private int weightIdx;
		private double ratio;

		public RandomSelect(Random random, double ratio, int weightIdx) {
			this.random = random;
			this.ratio = ratio;
			this.weightIdx = weightIdx;
			cuts = new ArrayList <>();
		}

		@Override
		public void open(Configuration configuration) {
			Tuple2 <Integer, double[]> bounds = (Tuple2 <Integer, double[]>) getRuntimeContext().getBroadcastVariable(
				BOUNDS).get(0);
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			double start = bounds.f1[taskId];
			double end = bounds.f1[taskId + 1];
			double sum = bounds.f1[bounds.f1.length - 1];
			end = Double.compare(end, sum) == 0 ? end + 0.1 : end;
			int sampleSize = (int) (bounds.f0 * ratio);
			int cnt = 0;
			while (cnt < sampleSize) {
				double cut = random.nextDouble() * sum;
				if (cut >= start && cut < end) {
					cuts.add(cut - start);
				}
				cnt++;
			}
			Collections.sort(cuts);
		}

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <Row> out) {
			if (cuts.size() == 0) {
				return;
			}
			double sum = 0.;
			int cnt = 0;
			double cur = cuts.get(cnt++);
			for (Row row : rows) {
				double weight = ((Number) row.getField(weightIdx)).doubleValue();
				while (sum + weight > cur) {
					out.collect(row);
					if (cnt < cuts.size()) {
						cur = cuts.get(cnt++);
					} else {
						return;
					}
				}
				sum += weight;
			}
		}
	}

	private static class TopNSelect extends RichMapPartitionFunction <Tuple2 <Double, Row>, Row> {
		private static final long serialVersionUID = -461361457193125904L;
		private double ratio;
		private List <Tuple3 <Integer, Integer, Double>> list;

		public TopNSelect(double ratio) {
			this.ratio = ratio;
		}

		@Override
		public void open(Configuration configuration) {
			List <Tuple3 <Integer, Integer, Double>> counts = getRuntimeContext().getBroadcastVariable(COUNT);
			list = new ArrayList <>();
			counts.forEach(list::add);
			Collections.sort(list, Comparator.comparingDouble(t -> -t.f2));
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Double, Row>> values, Collector <Row> out) {

			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int start = 0;
			int end = 0;
			int cnt = 0;
			for (Tuple3 <Integer, Integer, Double> t : list) {
				if (t.f0.equals(taskId)) {
					start = cnt;
					end = cnt + t.f1;
				}
				cnt += t.f1;
			}
			int sampleSize = (int) (cnt * ratio);
			if (start >= sampleSize) {
				return;
			} else if (end < sampleSize) {
				values.forEach(v -> out.collect(v.f1));
			} else {
				int size = sampleSize - start;
				PriorityQueue <Tuple2 <Double, Row>> queue = new PriorityQueue <>(
					Comparator.comparingDouble(t -> t.f0));
				double head = Double.MIN_VALUE;
				for (Tuple2 <Double, Row> t : values) {
					if (queue.size() < size) {
						queue.add(t);
						head = queue.peek().f0;
					} else {
						if (t.f0 > head) {
							queue.poll();
							queue.add(t);
							head = queue.peek().f0;
						}
					}
				}
				queue.forEach(v -> out.collect(v.f1));
			}
		}
	}
}
