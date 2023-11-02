package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.mapper.ComboModelMapper;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.batch.feature.WoeTrainBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorAssemblerMapper;
import com.alibaba.alink.operator.common.feature.binning.Bins;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.dataproc.vector.VectorAssemblerParams;
import com.alibaba.alink.params.feature.HasEncode;
import com.alibaba.alink.params.feature.HasEncode.Encode;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoe;
import com.alibaba.alink.params.feature.OneHotPredictParams;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.finance.BinningPredictParams;
import com.alibaba.alink.params.finance.WoePredictParams;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static com.alibaba.alink.params.feature.HasEncode.Encode.ASSEMBLED_VECTOR;

public class BinningModelMapper extends ComboModelMapper {

	private List <Mapper> mappers;
	private BinningPredictParamsBuilder paramsBuilder;

	public BinningModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		paramsBuilder = new BinningPredictParamsBuilder(params.clone(), dataSchema);
	}

	public static class BinningPredictParamsBuilder implements Serializable {
		public HasEncode.Encode encode;
		public String[] selectedCols;
		public String[] oneHotInputCols;
		public String[] oneHotOutputCols;
		public String[] quantileInputCols;
		public String[] quantileOutputCols;
		public String[] assemblerSelectedCols;

		public String[] resultCols;
		public TypeInformation <?>[] resultColTypes;
		public String[] reservedCols;

		public Params params;

		public BinningPredictParamsBuilder(Params params, TableSchema dataSchema) {
			selectedCols = params.get(BinningPredictParams.SELECTED_COLS);
			resultCols = params.get(BinningPredictParams.OUTPUT_COLS);
			reservedCols = params.get(BinningPredictParams.RESERVED_COLS);
			encode = params.get(BinningPredictParams.ENCODE);

			assemblerSelectedCols = new String[selectedCols.length];

			switch (encode) {
				case WOE: {
					params.set(BinningPredictParams.HANDLE_INVALID, HasHandleInvalid.HandleInvalid.KEEP);
					params.set(HasEncodeWithoutWoe.ENCODE, HasEncodeWithoutWoe.Encode.INDEX);
					if (resultCols == null) {
						resultCols = selectedCols;
					}
					Preconditions.checkArgument(resultCols.length == selectedCols.length,
						"OutputCols length must be equal to SelectedCols length!" );
					resultColTypes = new TypeInformation[selectedCols.length];
					Arrays.fill(resultColTypes, AlinkTypes.DOUBLE);
					break;
				}
				case INDEX: {
					if (resultCols == null) {
						resultCols = selectedCols;
					}
					Preconditions.checkArgument(resultCols.length == selectedCols.length,
						"OutputCols length must be equal to SelectedCols length!" );
					params.set(HasEncodeWithoutWoe.ENCODE, HasEncodeWithoutWoe.Encode.INDEX);
					resultColTypes = new TypeInformation[selectedCols.length];
					Arrays.fill(resultColTypes, AlinkTypes.LONG);
					break;
				}
				case VECTOR: {
					if (resultCols == null) {
						resultCols = selectedCols;
					}
					Preconditions.checkArgument(resultCols.length == selectedCols.length,
						"OutputCols length must be equal to SelectedCols length!" );
					params.set(HasEncodeWithoutWoe.ENCODE, HasEncodeWithoutWoe.Encode.VECTOR);
					resultColTypes = new TypeInformation[selectedCols.length];
					Arrays.fill(resultColTypes, AlinkTypes.SPARSE_VECTOR);
					break;
				}
				case ASSEMBLED_VECTOR: {
					params.set(HasEncodeWithoutWoe.ENCODE, HasEncodeWithoutWoe.Encode.VECTOR);
					Preconditions.checkArgument(null != resultCols && resultCols.length == 1,
						"When encode is ASSEMBLED_VECTOR, outputCols must be given and the length must be 1!" );
					params.set(HasOutputCol.OUTPUT_COL, resultCols[0]);
					resultCols = new String[selectedCols.length];
					for (int i = 0; i < selectedCols.length; i++) {
						resultCols[i] = selectedCols[i] + "_ASSEMBLED_VECTOR";
					}

					resultColTypes = new TypeInformation[] {AlinkTypes.SPARSE_VECTOR};
					break;
				}
				default: {
					throw new RuntimeException("Not support " + encode.name() + " yet!" );
				}
			}
			List <Integer> numericIndices = new ArrayList <>();
			List <Integer> discreteIndices = new ArrayList <>();
			for (int i = 0; i < selectedCols.length; i++) {
				if (TableUtil.isSupportedNumericType(TableUtil.findColTypeWithAssert(dataSchema, selectedCols[i]))) {
					numericIndices.add(i);
				} else {
					discreteIndices.add(i);
				}
			}

			quantileInputCols = new String[numericIndices.size()];
			quantileOutputCols = new String[numericIndices.size()];
			oneHotInputCols = new String[discreteIndices.size()];
			oneHotOutputCols = new String[discreteIndices.size()];

			for (int i = 0; i < numericIndices.size(); i++) {
				quantileInputCols[i] = selectedCols[numericIndices.get(i)];
				quantileOutputCols[i] = quantileInputCols[i] + "_QUANTILE";
				assemblerSelectedCols[TableUtil.findColIndexWithAssertAndHint(selectedCols, quantileInputCols[i])]
					= quantileOutputCols[i];
			}

			for (int i = 0; i < discreteIndices.size(); i++) {
				oneHotInputCols[i] = selectedCols[discreteIndices.get(i)];
				oneHotOutputCols[i] = oneHotInputCols[i] + "_ONE_HOT";
				assemblerSelectedCols[TableUtil.findColIndexWithAssertAndHint(selectedCols, oneHotInputCols[i])]
					= oneHotOutputCols[i];
			}
			this.params = params;
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		List <FeatureBinsCalculator> featureBinsCalculatorList = new BinningModelDataConverter().load(modelRows);
		HashSet <String> numeric = new HashSet <>();
		HashSet <String> discrete = new HashSet <>();
		//numeric, discrete
		Tuple2 <List <FeatureBinsCalculator>, List <FeatureBinsCalculator>> featureBorders =
			distinguishNumericDiscrete(
				featureBinsCalculatorList.toArray(new FeatureBinsCalculator[0]),
				params.get(BinningPredictParams.SELECTED_COLS),
				numeric,
				discrete);

		for (String s : params.get(BinningPredictParams.SELECTED_COLS)) {
			TypeInformation type = TableUtil.findColTypeWithAssert(this.getDataSchema(), s);
			Preconditions.checkNotNull(type, "%s is not found in data!", s);
			Preconditions.checkState((TableUtil.isSupportedNumericType(type) && numeric.contains(s)) ||
				(!TableUtil.isSupportedNumericType(type) && discrete.contains(s)), "%s is not found in model!", s);
		}

		mappers = new ArrayList <>();
		TableSchema dataSchema = getDataSchema();
		//discrete
		if (numeric.size() > 0) {
			Preconditions.checkState(featureBorders.f0.size() > 0,
				"There is numeric col that is not included in model, please check selectedCols!" );
			QuantileDiscretizerModelMapper quantileDiscretizerModelMapper = getQuantileModelMapper(featureBorders.f0,
				dataSchema, paramsBuilder);
			dataSchema = quantileDiscretizerModelMapper.getOutputSchema();
			mappers.add(quantileDiscretizerModelMapper);
		}

		//numeric
		if (discrete.size() > 0) {
			Preconditions.checkState(featureBorders.f1.size() > 0,
				"There is discrete col that is not included in model, please check selectedCols!" );
			OneHotModelMapper oneHotModelMapper = getOneHotModelMapper(
				featureBorders.f1,
				dataSchema,
				paramsBuilder);
			dataSchema = oneHotModelMapper.getOutputSchema();
			mappers.add(oneHotModelMapper);
		}

		//woe
		if (params.get(BinningPredictParams.ENCODE).equals(HasEncode.Encode.WOE)) {
			featureBorders.f0.addAll(featureBorders.f1);
			WoeModelMapper woeModelMapper = getWoeModelMapper(
				featureBorders.f0,
				dataSchema,
				paramsBuilder);
			dataSchema = woeModelMapper.getOutputSchema();
			mappers.add(woeModelMapper);
		}

		//vector assembler
		if (params.get(BinningPredictParams.ENCODE).equals(ASSEMBLED_VECTOR)) {
			VectorAssemblerMapper vectorAssemblerMapper = getVectorAssemblerMapper(
				dataSchema,
				paramsBuilder);
			dataSchema = vectorAssemblerMapper.getOutputSchema();
			mappers.add(vectorAssemblerMapper);
		}

		//final
		params.set(BINNING_INPUT_COLS, this.getDataSchema().getFieldNames());
		mappers.add(new BinningResultMapper(dataSchema, params));
	}

	@Override
	public List <Mapper> getLoadedMapperList() {
		return mappers;
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {
		String[] selectedCols = params.get(BinningPredictParams.SELECTED_COLS);
		String[] resultCols = params.get(BinningPredictParams.OUTPUT_COLS);
		if (resultCols == null) {
			resultCols = selectedCols;
		}
		String[] reservedCols = params.get(BinningPredictParams.RESERVED_COLS);

		Encode encode = params.get(BinningPredictParams.ENCODE);

		TypeInformation[] resultColTypes = new TypeInformation[resultCols.length];
		switch (encode) {
			case ASSEMBLED_VECTOR:
				Arrays.fill(resultColTypes, AlinkTypes.SPARSE_VECTOR);
				break;
			case WOE:
				Arrays.fill(resultColTypes, AlinkTypes.DOUBLE);
				break;
			case INDEX:
				Arrays.fill(resultColTypes, AlinkTypes.LONG);
				break;
			default:
				Arrays.fill(resultColTypes, AlinkTypes.SPARSE_VECTOR);
		}

		return Tuple4.of(selectedCols, resultCols, resultColTypes, reservedCols);
	}

	private static OneHotModelMapper getOneHotModelMapper(List <FeatureBinsCalculator> featureBinsCalculatorList,
														  TableSchema oneHotInputSchema,
														  BinningPredictParamsBuilder paramsBuilder) {

		OneHotModelMapper mapper = new OneHotModelMapper(new OneHotModelDataConverter().getModelSchema(),
			oneHotInputSchema, setOneHotModelParams(paramsBuilder));
		String[] selectedCols = new String[featureBinsCalculatorList.size()];
		List <Tuple2 <Long, FeatureBinsCalculator>> featureBorders = new ArrayList <>();
		for (int i = 0; i < featureBinsCalculatorList.size(); i++) {
			FeatureBinsCalculator featureBinsCalculator = featureBinsCalculatorList.get(i);
			selectedCols[i] = featureBinsCalculator.getFeatureName();
			featureBorders.add(Tuple2.of((long) i, featureBinsCalculator));
		}
		RowCollector modelRows = new RowCollector();
		Params meta = new Params().set(HasSelectedCols.SELECTED_COLS, selectedCols);
		OneHotTrainBatchOp.transformFeatureBinsToModel(featureBorders, modelRows, meta);
		mapper.loadModel(modelRows.getRows());
		return mapper;
	}

	private static QuantileDiscretizerModelMapper getQuantileModelMapper(
		List <FeatureBinsCalculator> featureBinsCalculatorList,
		TableSchema quantileInputSchema,
		BinningPredictParamsBuilder paramsBuilder) {

		QuantileDiscretizerModelMapper mapper = new QuantileDiscretizerModelMapper(
			new QuantileDiscretizerModelDataConverter().getModelSchema(), quantileInputSchema,
			setQuantileModelParams(paramsBuilder));
		RowCollector modelRows = new RowCollector();
		QuantileDiscretizerTrainBatchOp.transformFeatureBinsToModel(featureBinsCalculatorList, modelRows);
		mapper.loadModel(modelRows.getRows());
		return mapper;
	}

	private static WoeModelMapper getWoeModelMapper(List <FeatureBinsCalculator> featureBinsCalculatorList,
													TableSchema woeInputSchema,
													BinningPredictParamsBuilder paramsBuilder) {
		RowCollector modelRows = new RowCollector();
		Params meta = new Params().set(WoeTrainBatchOp.SELECTED_COLS, paramsBuilder.assemblerSelectedCols);
		transformFeatureBinsToModel(featureBinsCalculatorList, modelRows, meta, paramsBuilder.assemblerSelectedCols);
		WoeModelMapper mapper = new WoeModelMapper(new WoeModelDataConverter().getModelSchema(), woeInputSchema,
			setWoeModelParams(paramsBuilder, paramsBuilder.assemblerSelectedCols));
		mapper.loadModel(modelRows.getRows());
		return mapper;
	}

	private static VectorAssemblerMapper getVectorAssemblerMapper(TableSchema inputSchema,
																  BinningPredictParamsBuilder paramsBuilder) {
		return new VectorAssemblerMapper(inputSchema,
			setVectorAssemblerParams(paramsBuilder, paramsBuilder.assemblerSelectedCols, inputSchema.getFieldNames()));
	}

	private static Params setOneHotModelParams(BinningPredictParamsBuilder paramsBuilder) {
		return new Params()
			.merge(paramsBuilder.params)
			.set(OneHotPredictParams.SELECTED_COLS, paramsBuilder.oneHotInputCols)
			.set(OneHotPredictParams.OUTPUT_COLS, paramsBuilder.oneHotOutputCols)
			.set(OneHotPredictParams.RESERVED_COLS, null);
	}

	private static Params setQuantileModelParams(BinningPredictParamsBuilder paramsBuilder) {
		Params params = new Params()
			.merge(paramsBuilder.params)
			.set(QuantileDiscretizerPredictParams.SELECTED_COLS, paramsBuilder.quantileInputCols)
			.set(QuantileDiscretizerPredictParams.OUTPUT_COLS, paramsBuilder.quantileOutputCols)
			.set(QuantileDiscretizerPredictParams.RESERVED_COLS, null);
		return params;
	}

	private static Params setWoeModelParams(BinningPredictParamsBuilder paramsBuilder, String[] woeSelectedCols) {
		Params params = new Params()
			.merge(paramsBuilder.params)
			.set(WoePredictParams.SELECTED_COLS, woeSelectedCols)
			.set(WoePredictParams.OUTPUT_COLS, null)
			.set(WoePredictParams.RESERVED_COLS, null);
		return params;
	}

	private static Params setVectorAssemblerParams(BinningPredictParamsBuilder paramsBuilder,
												   String[] woeSelectedCols,
												   String[] binningInputCols) {

		Params params = new Params()
			.merge(paramsBuilder.params)
			.set(VectorAssemblerParams.SELECTED_COLS, woeSelectedCols)
			.set(VectorAssemblerParams.RESERVED_COLS, binningInputCols);
		switch (params.get(BinningPredictParams.HANDLE_INVALID)) {
			case KEEP:
			case SKIP: {
				params.set(VectorAssemblerParams.HANDLE_INVALID, VectorAssemblerParams.HandleInvalidMethod.SKIP);
				break;
			}
			case ERROR: {
				params.set(VectorAssemblerParams.HANDLE_INVALID, VectorAssemblerParams.HandleInvalidMethod.ERROR);
				break;
			}
		}
		return params;
	}

	public static Tuple2 <List <FeatureBinsCalculator>, List <FeatureBinsCalculator>> distinguishNumericDiscrete(
		FeatureBinsCalculator[] featureBinsCalculators,
		String[] selectedCols,
		HashSet <String> userDefinedNumeric,
		HashSet <String> userDefinedDiscrete) {
		List <FeatureBinsCalculator> numericFeatureBinsCalculator = new ArrayList <>();
		List <FeatureBinsCalculator> discreteFeatureBinsCalculator = new ArrayList <>();

		if (null != featureBinsCalculators) {
			for (FeatureBinsCalculator featureBinsCalculator : featureBinsCalculators) {
				if (TableUtil.findColIndex(selectedCols, featureBinsCalculator.getFeatureName()) >= 0) {
					if (featureBinsCalculator.isNumeric()) {
						numericFeatureBinsCalculator.add(featureBinsCalculator);
						userDefinedNumeric.add(featureBinsCalculator.getFeatureName());
					} else {
						discreteFeatureBinsCalculator.add(featureBinsCalculator);
						userDefinedDiscrete.add(featureBinsCalculator.getFeatureName());
					}
				}
			}
		}

		return Tuple2.of(numericFeatureBinsCalculator, discreteFeatureBinsCalculator);
	}

	public static Tuple2 <List <String>, List <String>> distinguishNumericDiscrete(
		String[] selectedCols,
		TableSchema dataSchema) {
		List <String> numeric = new ArrayList <>();
		List <String> discrete = new ArrayList <>();

		for (String s : selectedCols) {
			TypeInformation type = TableUtil.findColTypeWithAssert(dataSchema, s);
			Preconditions.checkNotNull(type, "%s is not found in data", s);
			if (TableUtil.isSupportedNumericType(type)) {
				numeric.add(s);
			} else {
				discrete.add(s);
			}
		}

		return Tuple2.of(numeric, discrete);
	}

	public static class BinningResultMapper extends Mapper {

		public BinningResultMapper(TableSchema dataSchema, Params params) {
			super(dataSchema, params);
		}

		@Override
		protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
			for (int i = 0; i < selection.length(); i++) {
				result.set(i, selection.get(i));
			}
		}

		@Override
		protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
			TableSchema dataSchema, Params params) {

			//binning input
			String[] binningInputCols = params.get(BINNING_INPUT_COLS);
			String[] selectedCols = params.get(BinningPredictParams.SELECTED_COLS);
			String[] reservedCols = params.get(BinningPredictParams.RESERVED_COLS);
			String[] resultCols = params.get(BinningPredictParams.OUTPUT_COLS);
			Encode encode = params.get(BinningPredictParams.ENCODE);

			reservedCols = reservedCols == null ? binningInputCols : reservedCols;
			resultCols = resultCols == null ? selectedCols : resultCols;

			BinningPredictParamsBuilder paramsBuilder = new BinningPredictParamsBuilder(params, dataSchema);

			String[] binningResultSelectedCols = null;
			switch (encode) {
				case ASSEMBLED_VECTOR:
					binningResultSelectedCols = new String[] {
						dataSchema.getFieldName(dataSchema.getFieldNames().length - 1).get()};
					break;
				case WOE:
				case INDEX:
				case VECTOR:
					binningResultSelectedCols = paramsBuilder.assemblerSelectedCols;
					break;
			}

			TypeInformation[] resultColTypes = new TypeInformation[resultCols.length];
			switch (encode) {
				case ASSEMBLED_VECTOR:
					Arrays.fill(resultColTypes, AlinkTypes.SPARSE_VECTOR);
					break;
				case WOE:
					Arrays.fill(resultColTypes, AlinkTypes.DOUBLE);
					break;
				case INDEX:
					Arrays.fill(resultColTypes, AlinkTypes.LONG);
					break;
				case VECTOR:
					Arrays.fill(resultColTypes, AlinkTypes.SPARSE_VECTOR);
					break;
			}

			return Tuple4.of(binningResultSelectedCols,
				resultCols,
				resultColTypes,
				reservedCols);
		}
	}

	public static void transformFeatureBinsToModel(Iterable <FeatureBinsCalculator> values, Collector <Row> out,
												   Params meta,
												   String[] featureNames) {
		Long positiveTotal = null;
		Long negativeTotal = null;
		List <Tuple4 <Integer, String, Long, Long>> list = new ArrayList <>();
		for (FeatureBinsCalculator featureBinsCalculator : values) {
			if (positiveTotal == null) {
				positiveTotal = featureBinsCalculator.getPositiveTotal();
				Preconditions.checkNotNull(positiveTotal, "The label col of Binning is not set!" );
				negativeTotal = featureBinsCalculator.getTotal() - positiveTotal;
			}
			String binFeatureName = featureBinsCalculator.getFeatureName();
			int featureIdx = TableUtil.findColIndex(featureNames, binFeatureName);
			if (featureIdx == -1) {
				featureIdx = TableUtil.findColIndex(featureNames,
					binFeatureName + "_ONE_HOT" );
			}
			if (featureIdx == -1) {
				featureIdx = TableUtil.findColIndex(featureNames,
					binFeatureName + "_QUANTILE" );
			}
			if (null != featureBinsCalculator.bin.nullBin) {
				list.add(getWoeModelTuple(featureBinsCalculator.bin.nullBin, featureIdx));
			}
			if (null != featureBinsCalculator.bin.elseBin) {
				list.add(getWoeModelTuple(featureBinsCalculator.bin.elseBin, featureIdx));
			}
			for (Bins.BaseBin bin : featureBinsCalculator.bin.normBins) {
				list.add(getWoeModelTuple(bin, featureIdx));
			}
		}
		if (null != meta) {
			meta.set(WoeModelDataConverter.POSITIVE_TOTAL, positiveTotal)
				.set(WoeModelDataConverter.NEGATIVE_TOTAL, negativeTotal);
		}
		new WoeModelDataConverter().save(Tuple2.of(meta, list), out);
	}

	private static Tuple4 <Integer, String, Long, Long> getWoeModelTuple(Bins.BaseBin bin, int featureIdx) {
		Long total = bin.getTotal();
		Long positive = bin.getPositive();
		return Tuple4.of(featureIdx, String.valueOf(bin.getIndex()), null == total ? 0L : total,
			null == positive ? 0L : positive);
	}

	static ParamInfo <String[]> BINNING_INPUT_COLS = ParamInfoFactory
		.createParamInfo("origin_data_cols", String[].class)
		.setDescription("origin data cols" )
		.build();
}
