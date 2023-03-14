package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.model.ModelDataConverter;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WoeModelDataConverter implements
	ModelDataConverter <Tuple2 <Params, Iterable <Tuple4 <Integer, String, Long, Long>>>, Map <String, Map <String,
		Double>>> {

	private static final String[] MODEL_COL_NAMES = new String[] {"featureIndex", "enumValue", "binTotal",
		"binPositiveTotal"};

	private static final TypeInformation[] MODEL_COL_TYPES = new TypeInformation[] {
		Types.LONG, Types.STRING, Types.LONG, Types.LONG};

	private static final TableSchema MODEL_SCHEMA = new TableSchema(MODEL_COL_NAMES, MODEL_COL_TYPES);

	public static ParamInfo <Long> POSITIVE_TOTAL = ParamInfoFactory
		.createParamInfo("positiveTotal", Long.class)
		.setDescription("positiveTotal")
		.setRequired()
		.build();

	public static ParamInfo <Long> NEGATIVE_TOTAL = ParamInfoFactory
		.createParamInfo("negativeTotal", Long.class)
		.setDescription("negativeTotal")
		.setRequired()
		.build();

	@Override
	public TableSchema getModelSchema() {
		return MODEL_SCHEMA;
	}

	@Override
	public Map <String, Map <String, Double>> load(List <Row> rows) {
		Map <String, Map <String, Double>> featureBinIndexTotalMap = new HashMap <>();
		String[] selectedCols = null;
		Long positiveTotal = null;
		Long negativeTotal = null;
		for (Row row : rows) {
			long colIndex = (Long) row.getField(0);
			if (colIndex < 0L) {
				Params params = Params.fromJson((String) row.getField(1));
				selectedCols = params.get(HasSelectedCols.SELECTED_COLS);
				positiveTotal = params.get(POSITIVE_TOTAL);
				negativeTotal = params.get(NEGATIVE_TOTAL);
				break;
			}
		}
		for (Row row : rows) {
			long colIndex = (Long) row.getField(0);
			if (colIndex >= 0L) {
				String featureName = selectedCols[(int) colIndex];
				Map <String, Double> map = featureBinIndexTotalMap.get(featureName);
				if (null != map) {
					map.put((String) row.getField(1), FeatureBinsUtil
						.calcWoe((long) row.getField(2), (long) row.getField(3), positiveTotal, negativeTotal));
				} else {
					map = new HashMap <>();
					map.put((String) row.getField(1), FeatureBinsUtil
						.calcWoe((long) row.getField(2), (long) row.getField(3), positiveTotal, negativeTotal));
					featureBinIndexTotalMap.put(featureName, map);
				}
			}
		}

		return featureBinIndexTotalMap;
	}

	@Override
	public void save(Tuple2 <Params, Iterable <Tuple4 <Integer, String, Long, Long>>> modelData,
					 Collector <Row> collector) {
		if (modelData.f0 != null) {
			collector.collect(Row.of(-1L, modelData.f0.toJson(), null, null));
		}
		modelData.f1.forEach(tuple -> {
			collector.collect(Row.of(tuple.f0.longValue(), tuple.f1, tuple.f2, tuple.f3));
		});
	}
}
