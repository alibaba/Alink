package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.HugeStringIndexerUtil;
import com.alibaba.alink.operator.common.dataproc.StringIndexerModelDataConverter;
import com.alibaba.alink.params.dataproc.SparseFeatureIndexerTrainParams;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;

@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("稀疏特征编码训练")
@NameEn("Sparse Feature Indexer Train")
public class SparseFeatureIndexerTrainBatchOp extends BatchOperator <SparseFeatureIndexerTrainBatchOp>
	implements SparseFeatureIndexerTrainParams <SparseFeatureIndexerTrainBatchOp> {

	private static final long serialVersionUID = 6127001361372601674L;

	public SparseFeatureIndexerTrainBatchOp() {
		this(new Params());
	}

	public SparseFeatureIndexerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public SparseFeatureIndexerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		String featureColName = getSelectedCol();
		int topN = getTopN();
		int minFrequency = getMinFrequency();
		String feaSplit = getSpareFeatureDelimiter();
		String feaValueSplit = getKvValDelimiter();
		boolean hasWeight = getHasValue();
		BatchOperator in = checkAndGetFirst(inputs);
		TypeInformation feaType = TableUtil.findColType(in.getSchema(), featureColName);
		if (!feaType.equals(Types.STRING)) {
			throw new AkIllegalDataException("featureColName type must be string, but input type is " + feaType);
		}
		DataSet <Row> dataset = in.select(featureColName).getDataSet();
		DataSet<Tuple2 <Integer, String>> feaFreSta = dataset.flatMap(
			new FlatMapFunction <Row, Tuple2 <Integer, String>>() {
				@Override
				public void flatMap(Row value, Collector <Tuple2 <Integer, String>> out) throws Exception {
					HashSet <String> set = new HashSet <>();
					String fea = (String) value.getField(0);
					String[] feaValues = fea.split(feaSplit);
					for (String feaValue : feaValues) {
						if (feaValue.length() == 0) {
							continue;
						}
						if (!hasWeight) {
							set.add(feaValue);
							out.collect(Tuple2.of(1, feaValue));
						} else {
							String[] feaAndValue = feaValue.split(feaValueSplit);
							if (feaAndValue.length <= 2 && feaAndValue[0].length() > 0) {
								if (set.contains(feaAndValue[0])) {
									continue;
								}
								set.add(feaAndValue[0]);
								out.collect(Tuple2.of(1, feaAndValue[0]));
							}
						}
					}
				}
			}).groupBy(1)
			.reduce(new ReduceFunction <Tuple2 <Integer, String>>() {
				@Override
				public Tuple2 <Integer, String> reduce(Tuple2 <Integer, String> value1,
													   Tuple2 <Integer, String> value2)
					throws Exception {
					return Tuple2.of(value1.f0 + value2.f0, value1.f1);
				}
			}).name("split_and_count_fea_frequency");
		this.setSideOutputTables(
			new Table[] {DataSetConversionUtil.toTable(getMLEnvironmentId(),feaFreSta.map(
				new MapFunction <Tuple2 <Integer, String>, Row>() {
					@Override
					public Row map(Tuple2 <Integer, String> value) throws Exception {
						return Row.of(value.f1, value.f0);
					}
				}) , new String[]{"feature", "feature_count"},
				new TypeInformation[]{Types.STRING, Types.INT})});
		if (minFrequency > 0) {
			feaFreSta = feaFreSta
				.filter(new FilterFunction <Tuple2 <Integer, String>>() {
					@Override
					public boolean filter(Tuple2 <Integer, String> value) throws Exception {
						return value.f0 >= minFrequency;
					}
				}).name("filter_less_frequency_fea");
		}

		if (topN > 0) {
			feaFreSta = feaFreSta.rebalance().mapPartition(
				new RichMapPartitionFunction <Tuple2 <Integer, String>, Tuple2 <Integer, String>>() {
					private static final long serialVersionUID = 2590378621506355139L;

					@Override
					public void mapPartition(Iterable <Tuple2 <Integer, String>> values,
											 Collector <Tuple2 <Integer, String>> out) throws Exception {
						final int totalTasks = getRuntimeContext().getNumberOfParallelSubtasks();
						final int currentSize = topN * 3 / totalTasks;
						Comparator<Tuple2 <Integer, String>> comparator = Comparator.comparingInt(o -> o.f0);
						PriorityQueue<Tuple2 <Integer, String>> queue = new PriorityQueue(comparator);
						Tuple2 <Integer, String> head = null;
						for (Tuple2 <Integer, String> v : values) {
							head = updateQueue(queue, currentSize, v, head);
						}
						Iterator <Tuple2 <Integer, String>> iterate = queue.iterator();
						while (iterate.hasNext()) {
							Tuple2 <Integer, String> v = iterate.next();
							out.collect(Tuple2.of(v.f0, v.f1));
						}
					}
				}).name("get_topn_fea_map")
				.reduceGroup(new RichGroupReduceFunction <Tuple2 <Integer, String>, Tuple2 <Integer, String>>() {
					@Override
					public void reduce(Iterable <Tuple2 <Integer, String>> values, Collector <Tuple2 <Integer, String>> out)
						throws Exception {
						Comparator<Tuple2 <Integer, String>> comparator = Comparator.comparingInt(o -> o.f0);
						PriorityQueue<Tuple2 <Integer, String>> queue = new PriorityQueue(comparator);
						Tuple2 <Integer, String> head = null;
						for (Tuple2 <Integer, String> v : values) {
							head = updateQueue(queue, topN, v, head);
						}
						Iterator <Tuple2 <Integer, String>> iterate = queue.iterator();
						while (iterate.hasNext()) {
							out.collect(iterate.next());
						}
					}
				}).name("get_topn_fea_reduce");
		}
		DataSet <Tuple3 <Integer, String, Long>> indexedToken = HugeStringIndexerUtil.indexSortedByAlphabet(
			feaFreSta.map(new MapFunction <Tuple2 <Integer, String>, Tuple2 <Integer, String>>() {
				@Override
				public Tuple2 <Integer, String> map(Tuple2 <Integer, String> value) throws Exception {
					return Tuple2.of(0, value.f1);
				}
			}), 0L, true);
		Params meta = new Params();
		meta.set(SparseFeatureIndexerTrainParams.HAS_VALUE, getHasValue());
		meta.set(SparseFeatureIndexerTrainParams.SPARSE_FEATURE_DELIMITER, getSpareFeatureDelimiter());
		meta.set(SparseFeatureIndexerTrainParams.KV_VAL_DELIMITER, getKvValDelimiter());
		DataSet <Row> values = indexedToken
			.mapPartition(new RichMapPartitionFunction <Tuple3 <Integer, String, Long>, Row>() {
				private static final long serialVersionUID = 8019085781267407813L;

				@Override
				public void mapPartition(Iterable <Tuple3 <Integer, String, Long>> values, Collector <Row> out)
					throws Exception {
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						out.collect(Row.of(meta.toJson(), null));
					}
					new StringIndexerModelDataConverter().save(values, out);
				}
			})
			.name("build_model")
			.returns(new RowTypeInfo(new StringIndexerModelDataConverter().getModelSchema().getFieldTypes()));

		this.setOutput(values, new StringIndexerModelDataConverter().getModelSchema());
		return this;
	}

	private static Tuple2 <Integer, String> updateQueue(PriorityQueue<Tuple2 <Integer, String>> queue,
														int size,
														Tuple2 <Integer, String> newValue,
														Tuple2 <Integer, String> head) {
		if (null == newValue) {
			return head;
		}
		if (queue.size() < size) {
			queue.add(Tuple2.of(newValue.f0, newValue.f1));
			head = queue.peek();
		} else {
			if (queue.comparator().compare(head, newValue) < 0) {
				Tuple2 <Integer, String> peek = queue.poll();
				peek.f0 = newValue.f0;
				peek.f1 = newValue.f1;
				queue.add(peek);
				head = queue.peek();
			}
		}
		return head;
	}
}
