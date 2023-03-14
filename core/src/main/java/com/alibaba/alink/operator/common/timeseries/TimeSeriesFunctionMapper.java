package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.mapper.MISOMapper;
import com.alibaba.alink.params.timeseries.TimeSeriesFunctionParams;
import com.alibaba.alink.params.timeseries.TimeSeriesFunctionParams.FuncName;

import java.util.ArrayList;
import java.util.List;

public class TimeSeriesFunctionMapper extends MISOMapper {

	private final FuncName funcName;

	public TimeSeriesFunctionMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.funcName = this.params.get(TimeSeriesFunctionParams.TIME_SERIES_FUNC_NAME);
	}

	@Override
	protected TypeInformation <?> initOutputColType() {
		switch (this.params.get(TimeSeriesFunctionParams.TIME_SERIES_FUNC_NAME)) {
			case Minus:
			case Minus_Abs:
				return AlinkTypes.M_TABLE;
			default:
				throw new RuntimeException("Function not support yet.");
		}
	}

	@Override
	protected Object map(Object[] input) {
		switch (this.funcName) {
			case Minus:
			case Minus_Abs:
				MTable origin = MTableUtil.getMTable(input[0]);
				MTable pred = MTableUtil.getMTable(input[1]);

				if (origin == null || pred == null) {
					return null;
				}

				origin.orderBy(0);
				pred.orderBy(0);

				List <Row> rows = new ArrayList <Row>();
				for (int i = 0; i < Math.min(pred.getNumRow(), origin.getNumRow()); i++) {
					Row originRow = origin.getRows().get(i);
					Row predRow = pred.getRows().get(i);
					double residual = (double) originRow.getField(1) - (double) predRow.getField(1);
					if (FuncName.Minus_Abs == this.funcName) {
						residual = Math.abs(residual);
					}

					rows.add(Row.of(originRow.getField(0), residual));
				}
				return new MTable(rows, origin.getSchemaStr());
			default:
				throw new RuntimeException("not support yet");
		}
	}
}
