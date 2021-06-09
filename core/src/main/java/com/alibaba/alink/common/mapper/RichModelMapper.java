package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.params.mapper.RichModelMapperParams;

/**
 * Abstract class for mappers with model.
 *
 * <p>The RichModel is used to the classification, the regression or the clustering.
 * The output of the model mapper using RichModel as its model contains three part:
 * <ul>
 * <li>The reserved columns from input</li>
 * <li>The prediction result column</li>
 * <li>The prediction detail column</li>
 * </ul>
 */
public abstract class RichModelMapper extends ModelMapper {

	private static final long serialVersionUID = -6722995426402759862L;

	/**
	 * The condition that the mapper output the prediction detail or not.
	 */
	private final boolean isPredDetail;

	public RichModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		isPredDetail = params.contains(RichModelMapperParams.PREDICTION_DETAIL_COL);
	}

	/**
	 * Initial the prediction result column type.
	 *
	 * <p>The subclass can override this method to initial the {@link OutputColsHelper}
	 *
	 * @return the type of the prediction result column
	 */
	protected TypeInformation <?> initPredResultColType(TableSchema modelSchema) {
		return modelSchema.getFieldTypes()[2];
	}

	@Override
	protected final Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {

		String[] selectedCols = dataSchema.getFieldNames();

		String[] outputCols;
		TypeInformation <?>[] outputTypes;
		TypeInformation <?> predResultColType = initPredResultColType(modelSchema);
		String predResultColName = params.get(RichModelMapperParams.PREDICTION_COL);
		boolean isPredDetail = params.contains(RichModelMapperParams.PREDICTION_DETAIL_COL);
		if (isPredDetail) {
			String predDetailColName = params.get(RichModelMapperParams.PREDICTION_DETAIL_COL);
			outputCols = new String[] {predResultColName, predDetailColName};
			outputTypes = new TypeInformation <?>[] {predResultColType, Types.STRING};
		} else {
			outputCols = new String[] {predResultColName};
			outputTypes = new TypeInformation <?>[] {predResultColType};
		}

		String[] reservedColNames = params.get(RichModelMapperParams.RESERVED_COLS);

		return Tuple4.of(selectedCols, outputCols, outputTypes, reservedColNames);
	}

	@Override
	protected final void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		if (isPredDetail) {
			Tuple2 <Object, String> t2 = predictResultDetail(selection);
			result.set(0, t2.f0);
			result.set(1, t2.f1);
		} else {
			result.set(0, predictResult(selection));
		}
	}

	/**
	 * Calculate the prediction result.
	 *
	 * @param selection the input
	 * @return the prediction result.
	 */
	protected abstract Object predictResult(SlicedSelectedSample selection) throws Exception;

	/**
	 * Calculate the prediction result ant the prediction detail.
	 *
	 * @param selection the input
	 * @return The prediction result and the the prediction detail.
	 */
	protected abstract Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception;

}
