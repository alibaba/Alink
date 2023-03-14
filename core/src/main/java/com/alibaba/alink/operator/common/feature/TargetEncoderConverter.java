package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.model.ModelDataConverter;

import java.util.HashMap;
import java.util.List;

public class TargetEncoderConverter
	implements ModelDataConverter <Row, TargetEncoderModelData> {
	public static final String SEPARATOR = "_____";
	private String[] selectedCols = null;

	public TargetEncoderConverter() {}

	public TargetEncoderConverter(String[] selectedCols) {
		this.selectedCols = selectedCols;
	}

	@Override
	public void save(Row modelData, Collector <Row> collector) {
		collector.collect(modelData);
	}

	@Override
	public TargetEncoderModelData load(List <Row> rows) {
		TargetEncoderModelData modelData = new TargetEncoderModelData();
		rows.forEach(modelData::setData);
		return modelData;
	}

	@Override
	public TableSchema getModelSchema() {
		StringBuilder sbd = new StringBuilder();
		sbd.append(selectedCols[0]);
		int size = selectedCols.length;
		for (int i = 1; i < size; i++) {
			sbd.append(SEPARATOR+selectedCols[i]);
		}
		String[] resCol = new String[] {"colName", sbd.toString()};
		TypeInformation[] resType = new TypeInformation[] {Types.STRING, Types.STRING};
		return new TableSchema(resCol, resType);
	}
}
