package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.SelectedColsWithFirstInputSpec;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.StringIndexerUtil;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;
import com.alibaba.alink.operator.common.feature.OneHotModelInfo;
import com.alibaba.alink.operator.common.feature.binning.BinDivideType;
import com.alibaba.alink.operator.common.feature.binning.Bins;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.dataproc.HasSelectedColTypes;
import com.alibaba.alink.params.feature.HasEnableElse;
import com.alibaba.alink.params.feature.OneHotTrainParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * One-hot maps a serial of columns of category indices to a column of sparse binary vector. It will produce a model of
 * one hot, and then it can transform data to binary format using this model.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@SelectedColsWithFirstInputSpec
@NameCn("独热编码训练")
public final class OneHotTrainBatchOp extends BatchOperator <OneHotTrainBatchOp>
	implements OneHotTrainParams <OneHotTrainBatchOp>,
	WithModelInfoBatchOp <OneHotModelInfo, OneHotTrainBatchOp, OneHotModelInfoBatchOp> {

	private static final long serialVersionUID = -4869233204093489524L;

	/**
	 * null constructor.
	 */
	public OneHotTrainBatchOp() {
		super(null);
	}

	/**
	 * constructor.
	 *
	 * @param params the parameters set.
	 */
	public OneHotTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public OneHotTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		final String[] selectedColNames = getSelectedCols();
		final String[] selectedColSqlType = new String[selectedColNames.length];
		for (int i = 0; i < selectedColNames.length; i++) {
			selectedColSqlType[i] = FlinkTypeConverter.getTypeString(
				TableUtil.findColTypeWithAssertAndHint(in.getSchema(), selectedColNames[i]));
		}
		int[] thresholdArray;

		if (!getParams().contains(OneHotTrainParams.DISCRETE_THRESHOLDS_ARRAY)) {
			thresholdArray = new int[selectedColNames.length];
			Arrays.fill(thresholdArray, getDiscreteThresholds());
		} else {
			thresholdArray = Arrays.stream(getDiscreteThresholdsArray()).mapToInt(Integer::intValue).toArray();
		}

		boolean enableElse = isEnableElse(thresholdArray);

		DataSet <Row> inputRows = in.select(selectedColNames).getDataSet();
		DataSet <Tuple3 <Integer, String, Long>> countTokens = StringIndexerUtil.countTokens(inputRows, true)
			.filter(new FilterFunction <Tuple3 <Integer, String, Long>>() {
				private static final long serialVersionUID = -8219708805787440332L;

				@Override
				public boolean filter(Tuple3 <Integer, String, Long> value) {
					return value.f2 >= thresholdArray[value.f0];
				}
			});

		DataSet <Tuple3 <Integer, String, Long>> indexedToken = StringIndexerUtil
			.zipWithIndexPerColumn((DataSet) countTokens.project(0, 1))
			.project(1, 2, 0);

		DataSet <Row> values = indexedToken
			.mapPartition(new RichMapPartitionFunction <Tuple3 <Integer, String, Long>, Row>() {
				private static final long serialVersionUID = 1218440919919078839L;

				@Override
				public void mapPartition(Iterable <Tuple3 <Integer, String, Long>> values, Collector <Row> out)
					throws Exception {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = new Params().set(HasSelectedCols.SELECTED_COLS, selectedColNames)
							.set(HasSelectedColTypes.SELECTED_COL_TYPES, selectedColSqlType)
							.set(HasEnableElse.ENABLE_ELSE, enableElse);
					}
					new OneHotModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			})
			.name("build_model");

		DataSet <Row> distinctNumber = indexedToken
			.groupBy(0)
			.aggregate(Aggregations.MAX, 2)
			.map(new MapFunction <Tuple3 <Integer, String, Long>, Row>() {
				private static final long serialVersionUID = 8508091938066560805L;

				@Override
				public Row map(Tuple3 <Integer, String, Long> value) throws Exception {
					return Row.of(selectedColNames[value.f0], value.f2 + 1);
				}
			});

		this.setOutput(values, new OneHotModelDataConverter().getModelSchema());
		this.setSideOutputTables(new Table[] {DataSetConversionUtil.toTable(getMLEnvironmentId(), distinctNumber,
			new String[] {"selectedCol", "distinctTokenNumber"}, new TypeInformation[] {Types.STRING, Types.LONG})});
		return this;
	}

	@Override
	public OneHotModelInfoBatchOp getModelInfoBatchOp() {
		return new OneHotModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

	/**
	 * If the thresholdArray is set and greater than 0, enableElse is true.
	 *
	 * @param thresholdArray thresholds for each column.
	 * @return enableElse.
	 */
	private static boolean isEnableElse(int[] thresholdArray) {
		for (int threshold : thresholdArray) {
			if (threshold > 0) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Transform OneHotModel to Binning Featureborder.
	 *
	 * @param modelDataSet OneHot model data.
	 * @return Featureborder for binning.
	 */
	public static DataSet <FeatureBinsCalculator> transformModelToFeatureBins(DataSet <Row> modelDataSet) {
		DataSet <Tuple2 <Long, FeatureBinsCalculator>> emptyFeatureBinsBuilder = modelDataSet.filter(
			new FilterFunction <Row>() {
				private static final long serialVersionUID = 6390346962976409046L;

				@Override
				public boolean filter(Row value) {
					return (long) value.getField(0) < 0;
				}
			}).flatMap(new FlatMapFunction <Row, Tuple2 <Long, FeatureBinsCalculator>>() {
			private static final long serialVersionUID = -6071069156554419492L;

			@Override
			public void flatMap(Row value, Collector <Tuple2 <Long, FeatureBinsCalculator>> collector)
				throws Exception {
				Params params = Params
					.fromJson((String) value.getField(1));
				String[] selectedCols = params.get(HasSelectedCols.SELECTED_COLS);
				String[] selectedColTypes = params.get(HasSelectedColTypes.SELECTED_COL_TYPES);
				for (int i = 0; i < selectedCols.length; i++) {
					Bins bins = new Bins();
					collector.collect(
						Tuple2.of((long) i, FeatureBinsCalculator.createDiscreteCalculator(BinDivideType.DISCRETE,
							selectedCols[i],
							FlinkTypeConverter.getFlinkType(selectedColTypes[i]), bins)));
				}
			}
		});

		DataSet <Tuple2 <Long, List <Row>>> data = modelDataSet
			.groupBy(0)
			.reduceGroup(new GroupReduceFunction <Row, Tuple2 <Long, List <Row>>>() {
				private static final long serialVersionUID = -6261110336454767751L;

				@Override
				public void reduce(Iterable <Row> values, Collector <Tuple2 <Long, List <Row>>> out) throws Exception {
					long colIndex = -1;
					List <Row> list = new ArrayList <>();
					for (Row row : values) {
						colIndex = (Long) row.getField(0);
						list.add(row);
					}
					if (colIndex >= 0) {
						out.collect(Tuple2.of(colIndex, list));
					}
				}
			});

		return emptyFeatureBinsBuilder
			.leftOuterJoin(data)
			.where(0)
			.equalTo(0)
			.with(
				new JoinFunction <Tuple2 <Long, FeatureBinsCalculator>, Tuple2 <Long, List <Row>>,
					FeatureBinsCalculator>() {
					private static final long serialVersionUID = -301364692111181603L;

					@Override
					public FeatureBinsCalculator join(Tuple2 <Long, FeatureBinsCalculator> first,
													  Tuple2 <Long, List <Row>> second) {
						FeatureBinsCalculator featureBinsCalculator = first.f1;
						if (second != null) {
							for (Row row : second.f1) {
								if (null == featureBinsCalculator.bin.normBins) {
									featureBinsCalculator.bin.normBins = new ArrayList <>();
								}
								Bins.BaseBin normBin = new Bins.BaseBin((long) row.getField(2),
									(String) row.getField(1));
								featureBinsCalculator.bin.normBins.add(normBin);
							}
						}
						return featureBinsCalculator;
					}
				});
	}

	/**
	 * Transform the binning model to onehot model.
	 *
	 * @param featureBorderDataSet binning model.
	 * @return onehot model.
	 */
	public static DataSet <Row> transformFeatureBinsToModel(DataSet <FeatureBinsCalculator> featureBorderDataSet) {
		DataSet <Tuple2 <Long, FeatureBinsCalculator>> featureBorderWithId = DataSetUtils.zipWithIndex(
			featureBorderDataSet);
		DataSet <Tuple3 <Long, String, String>> selectedCols = featureBorderWithId
			.map(new MapFunction <Tuple2 <Long, FeatureBinsCalculator>, Tuple3 <Long, String, String>>() {
				private static final long serialVersionUID = 888997785087051091L;

				@Override
				public Tuple3 <Long, String, String> map(Tuple2 <Long, FeatureBinsCalculator> value) throws Exception {
					return Tuple3.of(value.f0, value.f1.getFeatureName(), value.f1.getFeatureType());
				}
			});

		return featureBorderWithId.mapPartition(
			new RichMapPartitionFunction <Tuple2 <Long, FeatureBinsCalculator>, Row>() {
				private static final long serialVersionUID = -2674278791699432005L;

				@Override
				public void mapPartition(Iterable <Tuple2 <Long, FeatureBinsCalculator>> values, Collector <Row> out)
					throws Exception {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						List <Tuple3 <Long, String, String>> colList = getRuntimeContext().getBroadcastVariable(
							"selectedCols");
						String[] selectedCols = new String[colList.size()];
						String[] selectedColTypes = new String[colList.size()];
						colList.forEach(selectedCol -> {
							selectedCols[selectedCol.f0.intValue()] = selectedCol.f1;
							selectedColTypes[selectedCol.f0.intValue()] = selectedCol.f2;
						});
						meta = new Params().set(HasSelectedCols.SELECTED_COLS, selectedCols)
							.set(HasSelectedColTypes.SELECTED_COL_TYPES, selectedColTypes)
							.set(HasEnableElse.ENABLE_ELSE, true);
					}
					transformFeatureBinsToModel(values, out, meta);
				}
			}).withBroadcastSet(selectedCols, "selectedCols");
	}

	public static void transformFeatureBinsToModel(Iterable <Tuple2 <Long, FeatureBinsCalculator>> values,
												   Collector <Row> out,
												   Params meta) {
		List <Tuple3 <Integer, String, Long>> list = new ArrayList <>();
		for (Tuple2 <Long, FeatureBinsCalculator> featureBorder : values) {
			List <Bins.BaseBin> baseBins = featureBorder.f1.bin.normBins;
			for (Bins.BaseBin bin : baseBins) {
				for (String s : bin.getValues()) {
					list.add(
						Tuple3.of(featureBorder.f0.intValue(), s, bin.getIndex()));
				}
			}
		}
		new OneHotModelDataConverter().save(Tuple2.of(meta, list), out);
	}
}
