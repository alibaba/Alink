package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.feature.WoeTrainBatchOp;
import com.alibaba.alink.params.finance.WoePredictParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WoeModelMapper extends ModelMapper {
	private static final long serialVersionUID = 2784537716011869646L;
	private String[] selectedColNames;
	private List <Map <String, Double>> indexBinMap;
	private final Double defaultWoe;

	public WoeModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		defaultWoe = params.get(WoePredictParams.DEFAULT_WOE);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		selectedColNames = params.get(WoePredictParams.SELECTED_COLS);
		String[] outputColNames = params.get(WoePredictParams.OUTPUT_COLS);
		if (outputColNames == null) {
			outputColNames = selectedColNames;
		}
		Preconditions.checkArgument(outputColNames.length == selectedColNames.length,
			"OutputCol length must be equal to selectedCol length");
		String[] reservedColNames = params.get(WoePredictParams.RESERVED_COLS);

		TypeInformation[] outputColTypes = new TypeInformation[selectedColNames.length];
		Arrays.fill(outputColTypes, Types.DOUBLE);
		return Tuple4.of(selectedColNames, outputColNames, outputColTypes, reservedColNames);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		Map <String, Map <String, Double>> featureBinIndexWoeMap = new WoeModelDataConverter().load(modelRows);
		indexBinMap = new ArrayList <>();
		for (String s : selectedColNames) {
			Map <String, Double> map = featureBinIndexWoeMap.get(s);
			Preconditions.checkNotNull(map, "Can not find %s in model!", s);
			for (Map.Entry <String, Double> entry : map.entrySet()) {
				if (Double.isNaN(entry.getValue())) {
					map.put(entry.getKey(), defaultWoe);
				}
			}
			indexBinMap.add(map);
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < selectedColNames.length; i++) {
			Object bin = selection.get(i);
			Double woe = indexBinMap.get(i).get(null == bin ? WoeTrainBatchOp.NULL_STR : bin.toString());
			woe = null == woe ? defaultWoe : woe;
			Preconditions.checkArgument(!Double.isNaN(woe),
				"Woe is not set or is Nan for %s, you can provide default woe!", bin);
			result.set(i, woe);
		}
	}
}
