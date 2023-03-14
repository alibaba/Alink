package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.params.outlier.HasDetectLast;
import com.alibaba.alink.params.outlier.HasWithSeriesInfo;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

import java.util.Map;

import static com.alibaba.alink.operator.common.outlier.OutlierDetector.TEMP_MTABLE_COL;
import static com.alibaba.alink.operator.common.outlier.OutlierDetector.appendPreds2MTable;

public abstract class ModelOutlierWithSeriesDetector extends ModelMapper {

	/**
	 * The condition that the mapper output the prediction detail or not.
	 */
	private final boolean isPredDetail;
	private final boolean detectWithSeriesInfo;

	public ModelOutlierWithSeriesDetector(TableSchema modelSchema, TableSchema dataSchema,
										  Params params) {
		super(modelSchema, dataSchema, params);
		isPredDetail = params.contains(ModelOutlierWithSeriesDetectorParams.PREDICTION_DETAIL_COL);
		if (params.contains(HasWithSeriesInfo.WITH_SERIES_INFO)) {
			detectWithSeriesInfo = params.get(HasWithSeriesInfo.WITH_SERIES_INFO);
		} else {
			detectWithSeriesInfo = false;
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		if (detectWithSeriesInfo) {
			MTable mt = (MTable) selection.get(0);

			Tuple3 <Boolean, Double, Map <String, String>>[] preds = detectWithSeries(mt, params.get(HasDetectLast.DETECT_LAST));

			result.set(0, appendPreds2MTable(mt, preds, params, isPredDetail));
		} else {
			Row row = new Row(selection.length());
			selection.fillRow(row);
			Tuple3 <Boolean, Double, Map <String, String>> t2 = detectByModel(row);
			if (isPredDetail) {
				result.set(0, t2.f0);
				result.set(1, t2.f1);
			} else {
				result.set(0, t2.f0);
			}
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		if (params.contains(HasWithSeriesInfo.WITH_SERIES_INFO) && params.get(HasWithSeriesInfo.WITH_SERIES_INFO)) {
			return new Tuple4 <>(
				new String[] {TEMP_MTABLE_COL},
				new String[] {TEMP_MTABLE_COL},
				new TypeInformation <?>[] {AlinkTypes.M_TABLE},
				new String[0]
			);
		} else {
			String[] selectedCols = null;
			if (params.contains(HasSelectedCol.SELECTED_COL)) {
				selectedCols = new String[] {params.get(HasSelectedCol.SELECTED_COL)};
			} else {
				selectedCols = params.get(HasSelectedColsDefaultAsNull.SELECTED_COLS);
				if (null == selectedCols) {
					selectedCols = dataSchema.getFieldNames();
				}
			}
			String[] outputCols;
			TypeInformation <?>[] outputTypes;
			String predResultColName = params.get(ModelOutlierWithSeriesDetectorParams.PREDICTION_COL);
			boolean isPredDetail = params.contains(ModelOutlierWithSeriesDetectorParams.PREDICTION_DETAIL_COL);
			if (isPredDetail) {
				String predDetailColName = params.get(ModelOutlierWithSeriesDetectorParams.PREDICTION_DETAIL_COL);
				outputCols = new String[] {predResultColName, predDetailColName};
				outputTypes = new TypeInformation <?>[] {AlinkTypes.BOOLEAN, AlinkTypes.STRING};
			} else {
				outputCols = new String[] {predResultColName};
				outputTypes = new TypeInformation <?>[] {AlinkTypes.BOOLEAN};
			}
			return Tuple4.of(selectedCols, outputCols, outputTypes, new String[0]);
		}
	}

	public abstract Tuple3 <Boolean, Double, Map <String, String>>[] detectWithSeries(MTable series, boolean detectLast) throws Exception;

	public abstract Tuple3 <Boolean, Double, Map <String, String>> detectByModel(Row selection) throws Exception;
}
