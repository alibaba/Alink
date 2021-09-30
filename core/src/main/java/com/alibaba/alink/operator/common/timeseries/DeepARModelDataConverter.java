package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.timeseries.DeepARModelDataConverter.DeepARModelData;

import java.util.Collections;
import java.util.List;

public class DeepARModelDataConverter extends SimpleModelDataConverter <DeepARModelData, DeepARModelData> {
	public static final String DEEP_AR_INTERNAL_SCHEMA = "model_id long, model_info string";

	@Override
	public Tuple2 <Params, Iterable <String>> serializeModel(DeepARModelData modelData) {
		return Tuple2.of(
			modelData.meta,
			Collections.singletonList(
				new MTable(
					modelData.deepModel,
					CsvUtil.schemaStr2Schema(DEEP_AR_INTERNAL_SCHEMA)
				).toString())
		);
	}

	@Override
	public DeepARModelData deserializeModel(Params meta, Iterable <String> data) {
		return new DeepARModelData(
			meta,
			new MTable(data.iterator().next()).getTable()
		);
	}

	public static class DeepARModelData {
		public Params meta;
		public List <Row> deepModel;

		public DeepARModelData(Params meta, List <Row> deepModel) {
			this.meta = meta;
			this.deepModel = deepModel;
		}
	}
}
