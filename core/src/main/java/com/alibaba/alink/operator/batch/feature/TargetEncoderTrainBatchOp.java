package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.TargetEncoderConverter;
import com.alibaba.alink.operator.common.slidingwindow.SessionSharedData;
import com.alibaba.alink.params.feature.TargetEncoderTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.HashMap;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(PortType.MODEL)})
@ParamSelectColumnSpec(name = "selectedCols")
@ParamSelectColumnSpec(name = "labelCol")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.TargetEncoder")
@NameCn("目标编码训练")
@NameEn("Target Encoder Trainer")
public class TargetEncoderTrainBatchOp extends BatchOperator <TargetEncoderTrainBatchOp>
	implements TargetEncoderTrainParams <TargetEncoderTrainBatchOp> {

	public TargetEncoderTrainBatchOp(Params params) {
		super(params);
	}

	public TargetEncoderTrainBatchOp() {
		this(new Params());
	}

	@Override
	public TargetEncoderTrainBatchOp linkFrom(BatchOperator <?>... inputs) {

		BatchOperator <?> in = checkAndGetFirst(inputs);
		String label = getLabelCol();

		String[] selectedCols = getSelectedCols();
		if (selectedCols == null) {
			selectedCols = TableUtil
				.getCategoricalCols(in.getSchema(), in.getColNames(), null);
			ArrayList <String> listCols = new ArrayList <>();
			for (String s : selectedCols) {
				if (!s.equals(label)) {
					listCols.add(s);
				}
			}
			selectedCols = listCols.toArray(new String[0]);
		}
		int[] selectedColIndices = TableUtil.findColIndices(in.getSchema(), selectedCols);
		int labelIndex = TableUtil.findColIndex(in.getSchema(), label);
		String positiveLabel = getPositiveLabelValueString();
		int originalSize = in.getColNames().length;
		int selectedSize = selectedColIndices.length;
		int groupIndex = originalSize + selectedSize;
		DataSet <Tuple2 <Integer, Row>> inputData = in.getDataSet()
			.mapPartition(new MapInputData(selectedSize, originalSize))
			.map(new MapFunction <Row, Tuple2 <Integer, Row>>() {
				@Override
				public Tuple2 <Integer, Row> map(Row value) throws Exception {
					return Tuple2.of(0, value);
				}
			});

		DataSet <Tuple2 <Integer, Row>> res = MLEnvironmentFactory
			.get(getMLEnvironmentId())
			.getExecutionEnvironment()
			.fromElements(Tuple2.of(0, new Row(0)));

		// selectSize is iterator number.
		DeltaIteration <Tuple2 <Integer, Row>, Tuple2 <Integer, Row>> loop =
			res.iterateDelta(inputData, selectedSize, 0);

		// put select col value into groupIndex ceil, select col value is the iterator index.
		DataSet <Tuple2 <Integer, Row>> iterData = loop.getWorkset()
			.mapPartition(new BuildGroupByCol(groupIndex, selectedColIndices));

		// calc positive sum by group val.
		DataSet <Tuple2 <Object, Double>> means = iterData
			.groupBy(new RowKeySelector(groupIndex))
			.combineGroup(new CalcMean(groupIndex, labelIndex, positiveLabel))
			.groupBy(
				new KeySelector <Tuple4 <Object, Double, Integer, Double>, String>() {
					@Override
					public String getKey(Tuple4 <Object, Double, Integer, Double> tuple4) {
						return String.valueOf(tuple4.f0);
					}
				}
			)
			.reduceGroup(
				new GroupReduceFunction <Tuple4 <Object, Double, Integer, Double>, Tuple2 <Object, Double>>() {
					@Override
					public void reduce(Iterable <Tuple4 <Object, Double, Integer, Double>> iterable,
									   Collector <Tuple2 <Object, Double>> collector) throws Exception {
						double sum = 0;
						int count = 0;
						Object groupVal = null;
						for (Tuple4 <Object, Double, Integer, Double> it : iterable) {
							sum += it.f1;
							count += it.f2;
							groupVal = it.f0;
						}
						collector.collect(Tuple2.of(groupVal, sum / count));
					}
				});

		// < colIndex, Map<GroupVal, mean > .
		DataSet <Row> rowMeans = means
			// return <col, Map<groupVal, mean> > .
			.reduceGroup(new ReduceColumnInfo(selectedCols))
			.map(new MapFunction <Tuple2 <String, HashMap <String, Double>>, Row>() {
				@Override
				public Row map(Tuple2 <String, HashMap <String, Double>> value) throws Exception {
					Row res = new Row(2);
					res.setField(0, value.f0);
					res.setField(1, value.f1);
					return res;
				}
			}).returns(TypeInformation.of(Row.class));

		// put < col, Map<groupVal, mean> > into session obj.
		res = res
			.mapPartition(new BuildIterRes())
			.withBroadcastSet(rowMeans, "rowMeans");

		res = loop.closeWith(res, iterData);

		DataSet <Row> modelData = res
			// get < colIndex, Map<GroupVal, mean > from session obj.
			.mapPartition(new BuildModelData(selectedSize))
			.reduceGroup(new ReduceModelData());

		Table resTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), modelData,
			new TargetEncoderConverter(selectedCols).getModelSchema());

		this.setOutputTable(resTable);
		return this;
	}

	public static class RowKeySelector implements KeySelector <Tuple2 <Integer, Row>, String> {

		int index;

		public RowKeySelector(int index) {
			this.index = index;
		}

		@Override
		public String getKey(Tuple2 <Integer, Row> value) {
			return String.valueOf(value.f1.getField(index));
		}
	}

	public static class MapInputData implements MapPartitionFunction <Row, Row> {
		int selectedColSize;
		int originColSize;

		MapInputData(int selectedColSize, int originColSize) {
			this.selectedColSize = selectedColSize;
			this.originColSize = originColSize;
		}

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
			int resColSize = selectedColSize + originColSize + 1;//this is for groupBy
			Row res = new Row(resColSize);
			for (Row value : values) {
				for (int i = 0; i < originColSize; i++) {
					res.setField(i, value.getField(i));
				}
				out.collect(res);
			}
		}
	}

	public static class CalcMean
		implements GroupCombineFunction <Tuple2 <Integer, Row>, Tuple4 <Object, Double, Integer, Double>> {
		int groupIndex;
		int labelIndex;
		String positiveLabel;

		CalcMean(int groupIndex, int labelIndex, String positiveLabel) {
			this.groupIndex = groupIndex;
			this.labelIndex = labelIndex;
			this.positiveLabel = positiveLabel;
		}

		@Override
		public void combine(Iterable <Tuple2 <Integer, Row>> values,
							Collector <Tuple4 <Object, Double, Integer, Double>> out)
			throws Exception {
			int count = 0;
			double sum = 0;
			Object groupValue = null;
			for (Tuple2 <Integer, Row> value : values) {
				groupValue = value.f1.getField(groupIndex);
				++count;
				Object val = value.f1.getField(labelIndex);
				if (val != null) {
					if (positiveLabel == null) {
						sum += ((Number) val).doubleValue();
					} else if (String.valueOf(val).equals(positiveLabel)) {
						++sum;
					}
				}
			}

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("groupValue:" + groupValue + " sum: " + sum + " count: " + count);
			}

			out.collect(Tuple4.of(groupValue, sum, count, 0.0));
		}
	}

	public static class BuildGroupByCol
		extends RichMapPartitionFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, Row>> {
		int superStepNumber;
		int lastIndex;
		int[] selectedColIndices;

		BuildGroupByCol(int lastIndex, int[] selectedColIndices) {
			this.lastIndex = lastIndex;
			this.selectedColIndices = selectedColIndices;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			superStepNumber = getIterationRuntimeContext().getSuperstepNumber() - 1;
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, Row>> values,
								 Collector <Tuple2 <Integer, Row>> out) throws Exception {
			int selectedIndex = selectedColIndices[superStepNumber];
			for (Tuple2 <Integer, Row> value : values) {
				Object groupValue = value.f1.getField(selectedIndex);
				if (groupValue == null) {
					throw new AkIllegalDataException("select col must not have null value.");
				}
				value.f1.setField(lastIndex, groupValue);
				out.collect(value);
			}
		}
	}

	private static class ReduceColumnInfo
		extends RichGroupReduceFunction <Tuple2 <Object, Double>, Tuple2 <String, HashMap <String, Double>>> {
		String[] selectedCols;

		ReduceColumnInfo(String[] selectedCols) {
			this.selectedCols = selectedCols;
		}

		@Override
		public void reduce(Iterable <Tuple2 <Object, Double>> values,
						   Collector <Tuple2 <String, HashMap <String, Double>>> out) throws Exception {
			HashMap <String, Double> res = new HashMap <>();
			for (Tuple2 <Object, Double> value : values) {
				if (value.f0 != null) {
					res.put(value.f0.toString(), value.f1);
				} else {
					res.put(null, value.f1);
				}
			}
			int iterStepNum = getIterationRuntimeContext().getSuperstepNumber() - 1;
			out.collect(Tuple2.of(selectedCols[iterStepNum], res));
		}
	}

	private static class BuildIterRes extends RichMapPartitionFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, Row>> {
		Row items;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			items = (Row) getRuntimeContext().getBroadcastVariable("rowMeans").get(0);
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, Row>> values, Collector <Tuple2 <Integer, Row>> out)
			throws Exception {
			int superStepNum = getIterationRuntimeContext().getSuperstepNumber() - 1;
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int parallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
			if (superStepNum % parallelism == taskId) {
				SessionSharedData.put("" + superStepNum, taskId, items);
			}
		}
	}

	private static class BuildModelData
		extends RichMapPartitionFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, Row>> {
		int iterNum;

		BuildModelData(int iterNum) {
			this.iterNum = iterNum;
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, Row>> values,
								 Collector <Tuple2 <Integer, Row>> out) throws Exception {
			// wait previous operator to run
			values.iterator().hasNext();

			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int index = taskId;
			int parallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
			while (index < iterNum) {
				out.collect(Tuple2.of(0, (Row) SessionSharedData.get("" + index, taskId)));
				index += parallelism;
			}
		}
	}

	private static class ReduceModelData
		implements GroupReduceFunction <Tuple2 <Integer, Row>, Row> {

		@Override
		public void reduce(Iterable <Tuple2 <Integer, Row>> values,
						   Collector <Row> out) throws Exception {
			TargetEncoderConverter converter = new TargetEncoderConverter();
			for (Tuple2 <Integer, Row> value : values) {
				Row rowData = value.f1;
				rowData.setField(1, JsonConverter.toJson(rowData.getField(1)));
				converter.save(rowData, out);
			}
		}
	}
}
