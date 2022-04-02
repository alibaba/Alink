package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.params.outlier.HasMaxOutlierNumPerGroup;
import com.alibaba.alink.params.outlier.HasMaxOutlierRatio;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class OutlierDetector extends Mapper {

	public static final String TEMP_MTABLE_COL = "alink_outlier_temp_mtable_col";
	public static final String OUTLIER_SCORE_KEY = "outlier_score";
	public static final String IS_OUTLIER_KEY = "is_outlier";
	protected final Double outlierThreshold;
	protected final Double maxOutlierRatio;
	protected final Integer maxOutlierNumPerGroup;
	/**
	 * The condition that the mapper output the prediction detail or not.
	 */
	protected final boolean isPredDetail;

	public OutlierDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		isPredDetail = params.contains(OutlierDetectorParams.PREDICTION_DETAIL_COL);
		if (params.contains(OutlierDetectorParams.OUTLIER_THRESHOLD)) {
			outlierThreshold = params.get(OutlierDetectorParams.OUTLIER_THRESHOLD);
		} else {
			outlierThreshold = null;
		}
		if (params.contains(HasMaxOutlierRatio.MAX_OUTLIER_RATIO)) {
			this.maxOutlierRatio = params.get(HasMaxOutlierRatio.MAX_OUTLIER_RATIO);
		} else {
			this.maxOutlierRatio = null;
		}
		if (params.contains(HasMaxOutlierNumPerGroup.MAX_OUTLIER_NUM_PER_GROUP)) {
			this.maxOutlierNumPerGroup = params.get(HasMaxOutlierNumPerGroup.MAX_OUTLIER_NUM_PER_GROUP);
		} else {
			this.maxOutlierNumPerGroup = null;
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		MTable mt = (MTable) selection.get(0);

		Tuple3 <Boolean, Double, Map <String, String>>[] preds = detect(mt, params.get(HasDetectLast.DETECT_LAST));

		// NOTE:
		// outlier_score == Double.NEGATIVE_INFINITY means the point can not be outlier for any threshold and limits.

		if (null != maxOutlierRatio || null != maxOutlierNumPerGroup) {
			int m = preds.length;
			if (null != maxOutlierRatio) {
				m = Math.min(m, (int) Math.round(m * maxOutlierRatio));
			}
			if (null != maxOutlierNumPerGroup) {
				m = Math.min(m, maxOutlierNumPerGroup);
			}

			int countThreshold = 0;
			double threshold = (null == outlierThreshold) ? Double.NEGATIVE_INFINITY : outlierThreshold;
			ArrayList <Tuple2 <Integer, Double>> list = new ArrayList <>();
			for (int i = 0; i < preds.length; i++) {
				if (preds[i].f1 > threshold) {countThreshold++;}
				list.add(new Tuple2 <>(i, preds[i].f1));
			}
			m = Math.min(m, countThreshold);
			list.sort(new Comparator <Tuple2 <Integer, Double>>() {
				@Override
				public int compare(Tuple2 <Integer, Double> o1, Tuple2 <Integer, Double> o2) {
					return -o1.f1.compareTo(o2.f1);
				}
			});

			for (Tuple3 <Boolean, Double, Map <String, String>> pred : preds) {
				pred.f0 = false;
			}
			for (int i = 0; i < m; i++) {
				int index = list.get(i).f0;
				preds[index].f0 = true;
			}

		} else if (null != outlierThreshold) {
			for (Tuple3 <Boolean, Double, Map <String, String>> t3 : preds) {
				t3.f0 = t3.f1 > outlierThreshold;
			}
		}

		result.set(0, appendPreds2MTable(mt, preds, params, isPredDetail));
	}

	private static Row merge(Row input, Tuple3 <Boolean, Double, Map <String, String>> pred, boolean isPredDetail) {
		if (isPredDetail) {
			Map <String, String> map = null == pred.f2 ? new HashMap <>() : pred.f2;
			map.put(OUTLIER_SCORE_KEY, String.valueOf(pred.f1));
			map.put(IS_OUTLIER_KEY, String.valueOf(pred.f0));
			return RowUtil.merge(input, pred.f0, JsonConverter.toJson(map));
		} else {
			return RowUtil.merge(input, pred.f0);
		}
	}

	public static MTable appendPreds2MTable(MTable mt, Tuple3 <Boolean, Double, Map <String, String>>[] preds,
											Params params,
											boolean isPredDetail) {
		String schemaStr = mt.getSchemaStr() + ", " + params.get(OutlierDetectorParams.PREDICTION_COL) + " BOOLEAN";
		if (isPredDetail) {
			schemaStr = schemaStr + ", " + params.get(OutlierDetectorParams.PREDICTION_DETAIL_COL) + " STRING";
		}
		List <Row> rows = new ArrayList <>();
		if (mt.getNumRow() > 0) {
			if (params.get(HasDetectLast.DETECT_LAST)) {
				rows.add(
					merge(mt.getRow(mt.getNumRow() - 1), preds[preds.length - 1], isPredDetail)
				);
			} else {
				for (int i = 0; i < mt.getNumRow(); i++) {
					rows.add(merge(mt.getRow(i), preds[i], isPredDetail));
				}
			}
		}
		return new MTable(rows, schemaStr);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		return new Tuple4 <>(
			new String[] {params.get(HasInputMTableCol.INPUT_MTABLE_COL)},
			new String[] {params.get(HasOutputMTableCol.OUTPUT_MTABLE_COL)},
			new TypeInformation <?>[] {AlinkTypes.M_TABLE},
			dataSchema.getFieldNames()
		);
	}

	protected abstract Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast)
		throws Exception;
}
