package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.outlier.ModelOutlierParams;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.alink.operator.common.outlier.OutlierDetector.IS_OUTLIER_KEY;
import static com.alibaba.alink.operator.common.outlier.OutlierDetector.OUTLIER_SCORE_KEY;

public abstract class ModelOutlierDetector extends ModelMapper {

	/**
	 * The condition that the mapper output the prediction detail or not.
	 */
	protected final boolean isPredDetail;
	protected final Double OUTLIER_THRESHOLD;

	public ModelOutlierDetector(TableSchema modelSchema, TableSchema dataSchema,
								Params params) {
		super(modelSchema, dataSchema, params);
		isPredDetail = params.contains(ModelOutlierParams.PREDICTION_DETAIL_COL);
		if (params.contains(ModelOutlierParams.OUTLIER_THRESHOLD)) {
			OUTLIER_THRESHOLD = params.get(ModelOutlierParams.OUTLIER_THRESHOLD);
		} else {
			OUTLIER_THRESHOLD = null;
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		Tuple3 <Boolean, Double, Map <String, String>> pred = detect(selection);

		if (null != OUTLIER_THRESHOLD) {
			pred.f0 = pred.f1 > OUTLIER_THRESHOLD;
		}

		result.set(0, pred.f0);
		if (isPredDetail) {
			Map <String, String> map = null == pred.f2 ? new HashMap <>(0) : pred.f2;
			map.put(OUTLIER_SCORE_KEY, String.valueOf(pred.f1));
			map.put(IS_OUTLIER_KEY, String.valueOf(pred.f0));
			result.set(1, JsonConverter.toJson(map));
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		String[] selectedCols = dataSchema.getFieldNames();

		String[] outputCols;
		TypeInformation <?>[] outputTypes;
		String predResultColName = params.get(ModelOutlierParams.PREDICTION_COL);
		boolean isPredDetail = params.contains(ModelOutlierParams.PREDICTION_DETAIL_COL);
		if (isPredDetail) {
			String predDetailColName = params.get(ModelOutlierParams.PREDICTION_DETAIL_COL);
			outputCols = new String[] {predResultColName, predDetailColName};
			outputTypes = new TypeInformation <?>[] {AlinkTypes.BOOLEAN, AlinkTypes.STRING};
		} else {
			outputCols = new String[] {predResultColName};
			outputTypes = new TypeInformation <?>[] {AlinkTypes.BOOLEAN};
		}

		String[] reservedColNames = params.get(ModelOutlierParams.RESERVED_COLS);

		return Tuple4.of(selectedCols, outputCols, outputTypes, reservedColNames);
	}

	/**
	 * return Tuple3(isOutlier, probabilityOutlier, infoMap)
	 */
	protected abstract Tuple3 <Boolean, Double, Map <String, String>> detect(SlicedSelectedSample selection)
		throws Exception;
}
