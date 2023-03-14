package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ComboModelMapper;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.io.types.JdbcTypeConverter;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.params.classification.OneVsRestPredictParams;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.alibaba.alink.pipeline.classification.LinearSvm;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.classification.OneVsRest;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * ModelMapper for {@link OneVsRest}.
 */
public class OneVsRestModelMapper extends ComboModelMapper {

	private static final long serialVersionUID = 7008077848896699027L;

	private List <Mapper> mapperList;

	private static final String ONE_VS_REST_RESULT_VECTOR_COL_NAME = "one_vs_rest_result_vector_internal_implement";
	private static final String ONE_VS_REST_PRED_RESULT_COL_NAME = "one_vs_rest_pred_result_internal_implement";
	private static final String ONE_VS_REST_PRED_DETAIL_COL_NAME = "one_vs_rest_pred_detail_internal_implement";

	public OneVsRestModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		Params meta = extractMeta(modelRows);
		int numClasses = meta.get(ModelParamName.NUM_CLASSES);
		String labelsStr = meta.get(ModelParamName.LABELS);
		String labelTypeName = meta.get(ModelParamName.LABEL_TYPE_NAME);
		String binClsClassName = meta.get(ModelParamName.BIN_CLS_CLASS_NAME);
		String[] modelColNames = meta.get(ModelParamName.MODEL_COL_NAMES);
		Integer[] modelColTypesInt = meta.get(ModelParamName.MODEL_COL_TYPES);
		TypeInformation <?>[] modelColTypes = new TypeInformation[modelColTypesInt.length];
		for (int i = 0; i < modelColTypesInt.length; i++) {
			modelColTypes[i] = JdbcTypeConverter.getFlinkType(modelColTypesInt[i]);
		}

		mapperList = new ArrayList <>();

		String[] reserveColNames = getDataSchema().getFieldNames();

		Params binClsPredParams = params.clone()
			.set(OneVsRestPredictParams.RESERVED_COLS,
				ArrayUtils.add(reserveColNames, ONE_VS_REST_RESULT_VECTOR_COL_NAME))
			.set(OneVsRestPredictParams.PREDICTION_COL, ONE_VS_REST_PRED_RESULT_COL_NAME)
			.set(OneVsRestPredictParams.PREDICTION_DETAIL_COL, ONE_VS_REST_PRED_DETAIL_COL_NAME);

		Mapper initialResultMapper = new InitialResultMapper(
			getDataSchema(),
			params.clone()
				.set(OneVsRestPredictParams.RESERVED_COLS, reserveColNames)
				.set(ModelParamName.NUM_CLASSES, numClasses)
				.set(HasOutputCol.OUTPUT_COL, ONE_VS_REST_RESULT_VECTOR_COL_NAME)
		);

		mapperList.add(initialResultMapper);

		TableSchema setOutputSchema = null;

		try {
			for (int i = 0; i < numClasses; i++) {
				List <Row> rows = new ArrayList <>();
				for (Row row : modelRows) {
					if (row.getField(2) == null) {
						continue;
					}
					long id = (Long) row.getField(2);
					if ((long) (i) == id) {
						Row subRow = new Row(row.getArity() - 4);
						for (int j = 0; j < subRow.getArity(); j++) {
							subRow.setField(j, row.getField(3 + j));
						}
						rows.add(subRow);
					}
				}
				TableSchema schema = new TableSchema(modelColNames, modelColTypes);
				RichModelMapper predictor = createModelPredictor(
					binClsClassName,
					schema,
					initialResultMapper.getOutputSchema(),
					binClsPredParams,
					rows
				);

				Mapper setMapper = new SetPositiveResultMapper(
					predictor.getOutputSchema(),
					params.clone()
						.merge(binClsPredParams)
						.set(SetPositiveResultMapper.CLASS_INDEX, i)
						.set(HasVectorCol.VECTOR_COL, ONE_VS_REST_RESULT_VECTOR_COL_NAME)
						.set(HasOutputCol.OUTPUT_COL, ONE_VS_REST_RESULT_VECTOR_COL_NAME)
						.set(OneVsRestPredictParams.RESERVED_COLS, reserveColNames)
				);

				mapperList.add(predictor);
				mapperList.add(setMapper);
				setOutputSchema = setMapper.getOutputSchema();
			}
		} catch (Exception e) {
			throw new AkUnclassifiedErrorException("Error. ",e);
		}

		String[] reserveColNamesRaw = params.get(OneVsRestPredictParams.RESERVED_COLS);

		mapperList.add(new VoteMapper(
			setOutputSchema,
			params.clone()
				.set(ModelParamName.LABELS, labelsStr)
				.set(ModelParamName.LABEL_TYPE_NAME, labelTypeName)
				.set(HasVectorCol.VECTOR_COL, ONE_VS_REST_RESULT_VECTOR_COL_NAME)
				.set(
					OneVsRestPredictParams.RESERVED_COLS,
					null == reserveColNamesRaw ? reserveColNames : reserveColNamesRaw
				)
		));
	}

	@Override
	public List <Mapper> getLoadedMapperList() {
		return mapperList;
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {

		String predResultColName = params.get(OneVsRestPredictParams.PREDICTION_COL);
		String[] reserveColNames = params.get(OneVsRestPredictParams.RESERVED_COLS);
		int numModelCols = modelSchema.getFieldNames().length;
		TypeInformation <?> labelType = modelSchema.getFieldTypes()[numModelCols - 1];

		if (params.contains(OneVsRestPredictParams.PREDICTION_DETAIL_COL)) {
			return Tuple4.of(
				dataSchema.getFieldNames(),
				new String[] {
					predResultColName,
					params.get(OneVsRestPredictParams.PREDICTION_DETAIL_COL)
				},
				new TypeInformation <?>[] {labelType, Types.STRING},
				reserveColNames
			);
		} else {
			return Tuple4.of(
				dataSchema.getFieldNames(),
				new String[] {predResultColName},
				new TypeInformation <?>[] {labelType},
				reserveColNames
			);
		}
	}

	private static class InitialResultMapper extends Mapper {
		private final int numClasses;

		public InitialResultMapper(TableSchema dataSchema, Params params) {
			super(dataSchema, params);

			numClasses = params.get(ModelParamName.NUM_CLASSES);
		}

		@Override
		protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
			result.set(0, new DenseVector(numClasses));
		}

		@Override
		protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
			TableSchema dataSchema, Params params) {

			return Tuple4.of(
				dataSchema.getFieldNames(),
				new String[] {params.get(HasOutputCol.OUTPUT_COL)},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR},
				params.get(HasReservedColsDefaultAsNull.RESERVED_COLS)
			);
		}
	}

	private static class SetPositiveResultMapper extends Mapper {
		private final int classIndex;

		public static final ParamInfo <Integer> CLASS_INDEX = ParamInfoFactory
			.createParamInfo("classIndex", Integer.class)
			.build();

		public SetPositiveResultMapper(TableSchema dataSchema, Params params) {
			super(dataSchema, params);

			classIndex = params.get(CLASS_INDEX);
		}

		@Override
		protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
			Map <String, String> probMap = JsonConverter.fromJson(
				(String) selection.get(0),
				new TypeReference <Map <String, String>>() {}.getType()
			);
			DenseVector vector = VectorUtil.getDenseVector(selection.get(1));

			vector.set(classIndex, Double.parseDouble(probMap.get("1.0")));

			result.set(0, vector);
		}

		@Override
		protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
			TableSchema dataSchema, Params params) {

			return Tuple4.of(
				new String[] {
					params.get(OneVsRestPredictParams.PREDICTION_DETAIL_COL),
					params.get(HasVectorCol.VECTOR_COL)
				},
				new String[] {params.get(HasOutputCol.OUTPUT_COL)},
				new TypeInformation <?>[] {AlinkTypes.DENSE_VECTOR},
				params.get(HasReservedColsDefaultAsNull.RESERVED_COLS)
			);
		}
	}

	private static class VoteMapper extends Mapper {
		private final boolean predDetail;
		private final List <Object> labels;

		public VoteMapper(TableSchema dataSchema, Params params) {
			super(dataSchema, params);

			predDetail = params.contains(OneVsRestPredictParams.PREDICTION_DETAIL_COL);
			labels = recoverLabel(params.get(ModelParamName.LABELS), params.get(ModelParamName.LABEL_TYPE_NAME));
		}

		@Override
		protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {

			double[] score = VectorUtil.getDenseVector(selection.get(0)).getData();

			int maxIdx = -1;
			double maxProb = -Double.MAX_VALUE;
			double sum = 0.;
			for (int i = 0; i < score.length; i++) {
				sum += score[i];
				if (maxProb < score[i]) {
					maxIdx = i;
					maxProb = score[i];
				}
			}
			if (predDetail) {
				HashMap <Object, Double> details = new HashMap <>(labels.size());
				labels.forEach(label -> {
					details.put(label, 0.0);
				});

				for (int i = 0; i < score.length; i++) {
					details.replace(labels.get(i), score[i] / sum);
				}

				result.set(1, gson.toJson(details));
			}

			Object label = labels.get(maxIdx);
			result.set(0, label);
		}

		@Override
		protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
			TableSchema dataSchema, Params params) {

			String vectorColName = params.get(HasVectorCol.VECTOR_COL);
			String predResultColName = params.get(OneVsRestPredictParams.PREDICTION_COL);
			String[] reservedCols = params.get(OneVsRestPredictParams.RESERVED_COLS);

			TypeInformation <?> labelType = FlinkTypeConverter.getFlinkType(
				params.get(ModelParamName.LABEL_TYPE_NAME)
			);

			if (params.contains(OneVsRestPredictParams.PREDICTION_DETAIL_COL)) {

				return Tuple4.of(
					new String[] {vectorColName},
					new String[] {predResultColName, params.get(OneVsRestPredictParams.PREDICTION_DETAIL_COL)},
					new TypeInformation <?>[] {labelType, Types.STRING},
					reservedCols
				);

			} else {

				return Tuple4.of(
					new String[] {vectorColName},
					new String[] {predResultColName},
					new TypeInformation <?>[] {labelType},
					reservedCols
				);

			}
		}
	}

	private static void recoverLabelType(List <Object> labels, String labelTypeName) {
		// after jsonized and un-jsonized, Int, Long, Float objects all changes
		// to Double. So here we recover the label type.

		if (labelTypeName.equals(FlinkTypeConverter.getTypeString(Types.LONG))) {
			for (int i = 0; i < labels.size(); i++) {
				Double label = (Double) labels.get(i);
				labels.set(i, label.longValue());
			}
		} else if (labelTypeName.equals(FlinkTypeConverter.getTypeString(Types.INT))) {
			for (int i = 0; i < labels.size(); i++) {
				Double label = (Double) labels.get(i);
				labels.set(i, label.intValue());
			}
		} else if (labelTypeName.equals(FlinkTypeConverter.getTypeString(Types.FLOAT))) {
			for (int i = 0; i < labels.size(); i++) {
				Double label = (Double) labels.get(i);
				labels.set(i, label.floatValue());
			}
		}
	}

	private static RichModelMapper createModelPredictor(String binClsClssName, TableSchema modelSchema,
														TableSchema dataSchema,
														Params params, List <Row> modelRows) {
		RichModelMapper predictor;
		if (binClsClssName.equals(LogisticRegression.class.getCanonicalName()) ||
			binClsClssName.equals(LinearSvm.class.getCanonicalName())) {
			predictor = new LinearModelMapper(modelSchema, dataSchema, params);
			predictor.loadModel(modelRows);
		} else if (binClsClssName.equals(GbdtClassifier.class.getCanonicalName())) {
			predictor = new GbdtModelMapper(modelSchema, dataSchema, params);
			predictor.loadModel(modelRows);
		} else {
			throw new UnsupportedOperationException("OneVsRest does not support classifier: " + binClsClssName);
		}
		return predictor;
	}

	public Params extractMeta(List <Row> modelRows) {
		Params meta = null;
		for (Row row : modelRows) {
			if (row.getField(1) != null) {
				meta = Params.fromJson((String) row.getField(1));
				break;
			}
		}
		return meta;
	}

	private static List <Object> recoverLabel(String labelsStr, String labelTypeName) {
		List <Object> labels = gson.fromJson(labelsStr, ArrayList.class);
		recoverLabelType(labels, labelTypeName);

		return labels;
	}

}
