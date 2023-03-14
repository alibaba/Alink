package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.feature.WoeModelDataConverter;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.dataproc.HasSelectedColTypes;
import com.alibaba.alink.params.finance.WoeTrainParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.commons.lang3.ArrayUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
	@PortSpec(PortType.MODEL)
})

@ParamSelectColumnSpec(name = "selectedCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "labelCol")

@Internal
public final class WoeTrainBatchOp extends BatchOperator <WoeTrainBatchOp>
	implements WoeTrainParams <WoeTrainBatchOp> {

	private static final long serialVersionUID = 5413307707249156884L;
	public static String NULL_STR = "WOE_NULL_STRING";

	public WoeTrainBatchOp() {}

	public WoeTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public WoeTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator in = checkAndGetFirst(inputs);
		String[] selectedCols = this.getSelectedCols();
		final String[] selectedColSqlType = new String[selectedCols.length];
		for (int i = 0; i < selectedCols.length; i++) {
			selectedColSqlType[i] = FlinkTypeConverter.getTypeString(
				TableUtil.findColType(in.getSchema(), selectedCols[i]));
		}
		int selectedLen = selectedCols.length;
		String labelCol = this.getLabelCol();
		TypeInformation type = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelCol);
		DataSet <Tuple2 <String, Long>> labelCount = in.select(labelCol).getDataSet().map(
			new MapFunction <Row, Tuple2 <String, Long>>() {
				private static final long serialVersionUID = 7244041023667742068L;

				@Override
				public Tuple2 <String, Long> map(Row value) throws Exception {
					Preconditions.checkNotNull(value.getField(0), "LabelCol contains null value!");
					return Tuple2.of(
						new EvaluationUtil.ComparableLabel(value.getField(0).toString(), type).label.toString(), 1L);
				}
			})
			.groupBy(0)
			.sum(1);
		EvaluationUtil.ComparableLabel positiveValue = new EvaluationUtil.ComparableLabel(
			this.getPositiveLabelValueString(), type);

		DataSet <Tuple4 <Integer, String, Long, Long>> total = in.select(ArrayUtils.add(selectedCols, labelCol))
			.getDataSet()
			.flatMap(new FlatMapFunction <Row, Tuple3 <Integer, String, Long>>() {
				private static final long serialVersionUID = 3246134198223500699L;

				@Override
				public void flatMap(Row value, Collector <Tuple3 <Integer, String, Long>> out) {
					Long equalPositive = new EvaluationUtil.ComparableLabel(value.getField(selectedLen), type).equals(
						positiveValue) ? 1L : 0L;
					for (int i = 0; i < selectedLen; i++) {
						Object obj = value.getField(i);
						out.collect(Tuple3.of(i, null == obj ? NULL_STR : obj.toString(), equalPositive));
					}
				}
			})
			.groupBy(0, 1)
			.reduceGroup(
				new GroupReduceFunction <Tuple3 <Integer, String, Long>, Tuple4 <Integer, String, Long, Long>>() {
					private static final long serialVersionUID = 8132981693511963253L;

					@Override
					public void reduce(Iterable <Tuple3 <Integer, String, Long>> values,
									   Collector <Tuple4 <Integer, String, Long, Long>> out) throws Exception {
						Long binPositiveTotal = 0L;
						Long binTotal = 0L;
						int colIdx = -1;
						String binIndex = null;
						for (Tuple3 <Integer, String, Long> t : values) {
							binTotal++;
							colIdx = t.f0;
							binIndex = t.f1;
							binPositiveTotal += t.f2;
						}
						if (colIdx >= 0) {
							out.collect(Tuple4.of(colIdx, binIndex, binTotal, binPositiveTotal));
						}
					}
				});
		DataSet <Row> values = total
			.mapPartition(new RichMapPartitionFunction <Tuple4 <Integer, String, Long, Long>, Row>() {
				private static final long serialVersionUID = 9015674191729072450L;
				private long positiveTotal;
				private long negativeTotal;

				@Override
				public void open(Configuration configuration) {
					List <Tuple2 <String, Long>> labelCount = this.getRuntimeContext().getBroadcastVariable(
						"labelCount");
					Preconditions.checkArgument(labelCount.size() == 2, "Only support binary classification!");
					if (positiveValue.equals(new EvaluationUtil.ComparableLabel(labelCount.get(0).f0, type))) {
						positiveTotal = labelCount.get(0).f1;
						negativeTotal = labelCount.get(1).f1;
					} else if (positiveValue.equals(new EvaluationUtil.ComparableLabel(labelCount.get(1).f0, type))) {
						positiveTotal = labelCount.get(1).f1;
						negativeTotal = labelCount.get(0).f1;
					} else {
						throw new IllegalArgumentException("Not contain positiveValue " + positiveValue);
					}
				}

				@Override
				public void mapPartition(Iterable <Tuple4 <Integer, String, Long, Long>> values, Collector <Row> out) {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = new Params()
							.set(HasSelectedCols.SELECTED_COLS, selectedCols)
							.set(HasSelectedColTypes.SELECTED_COL_TYPES, selectedColSqlType)
							.set(WoeModelDataConverter.POSITIVE_TOTAL, positiveTotal)
							.set(WoeModelDataConverter.NEGATIVE_TOTAL, negativeTotal);
					}
					new WoeModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			})
			.withBroadcastSet(labelCount, "labelCount")
			.name("build_model");

		this.setOutput(values, new WoeModelDataConverter().getModelSchema());
		return this;
	}

	public static DataSet <FeatureBinsCalculator> setFeatureBinsWoe(
		DataSet <FeatureBinsCalculator> featureBorderDataSet,
		DataSet <Row> woeModel) {
		DataSet <Tuple2 <String, FeatureBinsCalculator>> borderWithName = featureBorderDataSet.map(
			new MapFunction <FeatureBinsCalculator, Tuple2 <String, FeatureBinsCalculator>>() {
				private static final long serialVersionUID = 3810414585464772028L;

				@Override
				public Tuple2 <String, FeatureBinsCalculator> map(FeatureBinsCalculator value) {
					return Tuple2.of(value.getFeatureName(), value);
				}
			});

		DataSet <Row> selectedCols = woeModel.filter(new FilterFunction <Row>() {
			private static final long serialVersionUID = -2272981616877035934L;

			@Override
			public boolean filter(Row value) {
				return (long) value.getField(0) < 0;
			}
		});

		DataSet <Tuple3 <String, Map <Long, Long>, Map <Long, Long>>> featureCounts = woeModel
			.groupBy(0)
			.reduceGroup(new RichGroupReduceFunction <Row, Tuple3 <String, Map <Long, Long>, Map <Long, Long>>>() {
				private static final long serialVersionUID = 2877684088626081532L;

				@Override
				public void reduce(Iterable <Row> values,
								   Collector <Tuple3 <String, Map <Long, Long>, Map <Long, Long>>> out) {
					String[] selectedCols = Params
						.fromJson((String) ((Row) getRuntimeContext()
							.getBroadcastVariable("selectedCols")
							.get(0))
							.getField(1))
						.get(WoeTrainParams.SELECTED_COLS);

					Map <Long, Long> total = new HashMap <>();
					Map <Long, Long> positiveTotal = new HashMap <>();
					long colIndex = -1;
					for (Row row : values) {
						colIndex = (Long) row.getField(0);
						if (colIndex >= 0L) {
							Long key = Long.valueOf((String) row.getField(1));
							total.put(key, (long) row.getField(2));
							positiveTotal.put(key, (long) row.getField(3));
						} else {
							return;
						}
					}
					out.collect(Tuple3.of(selectedCols[(int) colIndex], total, positiveTotal));
				}
			}).withBroadcastSet(selectedCols, "selectedCols")
			.name("GetBinTotalFromWoeModel");

		return borderWithName
			.join(featureCounts)
			.where(0)
			.equalTo(0)
			.with(
				new JoinFunction <Tuple2 <String, FeatureBinsCalculator>, Tuple3 <String, Map <Long, Long>, Map <Long,
					Long>>,
					FeatureBinsCalculator>() {
					private static final long serialVersionUID = -4468441310215491228L;

					@Override
					public FeatureBinsCalculator join(Tuple2 <String, FeatureBinsCalculator> first,
													  Tuple3 <String, Map <Long, Long>, Map <Long, Long>> second) {
						FeatureBinsCalculator border = first.f1;
						border.setTotal(second.f1);
						border.setPositiveTotal(second.f2);
						return border;
					}
				}).name("SetBinTotal");
	}


}
