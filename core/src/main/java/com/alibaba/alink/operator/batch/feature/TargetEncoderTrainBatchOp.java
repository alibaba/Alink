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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
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
@NameCn("TargetEncoder")
@NameEn("TargetEncoder")
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

		BatchOperator in = checkAndGetFirst(inputs);
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

		DeltaIteration <Tuple2 <Integer, Row>, Tuple2 <Integer, Row>> loop =
			res.iterateDelta(inputData, selectedSize, 0);
		DataSet <Tuple2 <Integer, Row>> iterData = loop.getWorkset()
			.mapPartition(new BuildGroupByCol(groupIndex, selectedColIndices));
		DataSet <Tuple2 <Object, Double>> means = iterData
			.groupBy(new RowKeySelector(groupIndex))
			.combineGroup(new CalcMean(groupIndex, labelIndex, positiveLabel));
		DataSet <Row> rowMeans = means
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

		res = res
			.mapPartition(new BuildIterRes())
			.withBroadcastSet(rowMeans, "rowMeans");

		res = loop.closeWith(res, iterData);

		DataSet <Row> modelData = res
			.mapPartition(new BuildModelData(selectedSize))
			.reduceGroup(new ReduceModelData());

		Table resTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), modelData,
			new TargetEncoderConverter(selectedCols).getModelSchema());
		this.setOutputTable(resTable);
		return this;
	}

	public static class RowKeySelector implements KeySelector <Tuple2 <Integer, Row>, Comparable> {

		int index;

		public RowKeySelector(int index) {
			this.index = index;
		}

		@Override
		public Comparable getKey(Tuple2 <Integer, Row> value) {
			return (Comparable) (value.f1.getField(index));
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

	public static class CalcMean implements GroupCombineFunction <Tuple2 <Integer, Row>, Tuple2 <Object, Double>> {
		int groupIndex;
		int labelIndex;
		String positiveLabel;

		CalcMean(int groupIndex, int labelIndex, String positiveLabel) {
			this.groupIndex = groupIndex;
			this.labelIndex = labelIndex;
			this.positiveLabel = positiveLabel;
		}

		@Override
		public void combine(Iterable <Tuple2 <Integer, Row>> values, Collector <Tuple2 <Object, Double>> out)
			throws Exception {
			int count = 0;
			double sum = 0;
			Object groupValue = null;
			for (Tuple2 <Integer, Row> value : values) {
				groupValue = value.f1.getField(groupIndex);
				++count;
				if (positiveLabel == null) {
					sum += (double) value.f1.getField(labelIndex);
				} else if (value.f1.getField(labelIndex).toString().equals(positiveLabel)) {
					++sum;
				}
			}
			double mean = sum / count;
			out.collect(Tuple2.of(groupValue, mean));
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
				value.f1.setField(lastIndex, value.f1.getField(selectedIndex));
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
				res.put(value.f0.toString(), value.f1);
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
		public void mapPartition(Iterable <Tuple2 <Integer, Row>> values, Collector <Tuple2 <Integer, Row>> out) throws Exception {
			int superStepNum = getIterationRuntimeContext().getSuperstepNumber() - 1;
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int parallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
			if (superStepNum % parallelism == taskId) {
				SessionSharedData.put("" + superStepNum, taskId, items);
			}
		}
	}

	private static class BuildModelData
		extends RichMapPartitionFunction <Tuple2 <Integer, Row>, Tuple2<Integer, Row>> {
		int iterNum;

		BuildModelData(int iterNum) {
			this.iterNum = iterNum;
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, Row>> values,
								 Collector <Tuple2<Integer, Row>> out) throws Exception {
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int index = taskId;
			int parallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
			while (index < iterNum) {
				out.collect(Tuple2.of(0, (Row) SessionSharedData.get(""+index, taskId)));
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
