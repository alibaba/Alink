package com.alibaba.alink.operator.common.regression;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.regression.glm.FamilyLink;
import com.alibaba.alink.operator.common.regression.glm.GlmUtil;
import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.regression.GlmPredictParams;
import com.alibaba.alink.params.regression.GlmTrainParams;

import java.util.List;

/**
 * GLM model mapper for glm predict.
 */
public class GlmModelMapper extends ModelMapper {

	private static final long serialVersionUID = 8193374524901551398L;
	/**
	 * coefficients of each features.
	 */
	private double[] coefficients;

	/**
	 * intercept.
	 */
	private double intercept;

	/**
	 * family and link function.
	 */
	private FamilyLink familyLink;

	/**
	 * offset col idx.
	 */
	private int offsetColIdx;

	/**
	 * feature col indices.
	 */
	private int[] featureColIdxs;

	/**
	 * if need calculate link predict col.
	 */
	private boolean hasLinkPredit;

	/**
	 * @param modelSchema
	 * @param dataSchema
	 * @param params
	 */
	public GlmModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		String linkPredResultColName = params.get(GlmPredictParams.LINK_PRED_RESULT_COL);

		hasLinkPredit = linkPredResultColName != null && !linkPredResultColName.isEmpty();
	}

	/**
	 * @param modelRows the list of Row type data
	 */
	@Override
	public void loadModel(List <Row> modelRows) {
		GlmModelDataConverter model = new GlmModelDataConverter();
		GlmModelData modelData = model.load(modelRows);

		coefficients = modelData.coefficients;
		intercept = modelData.intercept;
		TableSchema dataSchema = getDataSchema();
		if (modelData.offsetColName == null || modelData.offsetColName.isEmpty()) {
			offsetColIdx = -1;
		} else {
			offsetColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), modelData.offsetColName);
		}

		featureColIdxs = new int[modelData.featureColNames.length];
		for (int i = 0; i < featureColIdxs.length; i++) {
			featureColIdxs[i] = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(),
				modelData.featureColNames[i]);
		}

		GlmTrainParams.Family familyName = modelData.familyName;
		double variancePower = modelData.variancePower;
		GlmTrainParams.Link linkName = modelData.linkName;
		double linkPower = modelData.linkPower;

		familyLink = new FamilyLink(familyName, variancePower, linkName, linkPower);

	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		String[] resultColNames;
		TypeInformation[] resultColTypes;
		String predResultColName = params.get(GlmPredictParams.PREDICTION_COL);
		String linkPredResultColName = params.get(GlmPredictParams.LINK_PRED_RESULT_COL);
		if (linkPredResultColName == null || linkPredResultColName.isEmpty()) {
			resultColNames = new String[] {predResultColName};
			resultColTypes = new TypeInformation[] {Types.DOUBLE()};
		} else {
			resultColNames = new String[] {predResultColName, linkPredResultColName};
			resultColTypes = new TypeInformation[] {Types.DOUBLE(), Types.DOUBLE()};
		}
		return Tuple4.of(
			this.getDataSchema().getFieldNames(),
			resultColNames,
			resultColTypes,
			params.get(RichModelMapperParams.RESERVED_COLS)
		);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		double offset = 0;
		if (offsetColIdx >= 0) {
			offset = ((Number) selection.get(offsetColIdx)).doubleValue();
		}

		double[] features = new double[featureColIdxs.length];
		for (int i = 0; i < features.length; i++) {
			features[i] = ((Number) selection.get(featureColIdxs[i])).doubleValue();
		}

		double predict = GlmUtil.predict(coefficients, intercept, features, offset, familyLink);

		if (hasLinkPredit) {
			double eta = GlmUtil.linearPredict(coefficients, intercept, features) + offset;
			result.set(0, predict);
			result.set(1, eta);
		} else {
			result.set(0, predict);
		}
	}

}
