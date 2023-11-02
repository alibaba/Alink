package com.alibaba.alink.operator.stream.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.utils.TimeUtil;
import com.alibaba.alink.operator.common.statistics.basicstatistic.QuantileWindowFunction;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.shared.HasTimeCol_null;
import com.alibaba.alink.params.statistics.QuantileParams;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@NameCn("分位数")
@NameEn("Quantile")
public final class QuantileStreamOp extends StreamOperator <QuantileStreamOp>
	implements QuantileParams <QuantileStreamOp> {

	private static final long serialVersionUID = 8927492832239574864L;

	public QuantileStreamOp() {
		super();
	}

	public QuantileStreamOp(Params params) {
		super(params);
	}

	@Override
	public QuantileStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		String[] selectedColNames = getSelectedCols();
		if (null == selectedColNames) {
			selectedColNames = in.getColNames();
		}
		double timeInterval = getTimeInterval();
		int qN = getQuantileNum();
		String timeColName = getParams().get(HasTimeCol_null.TIME_COL);
		int delayTime = getDalayTime();

		checkParameter(selectedColNames, timeInterval, qN, timeColName, delayTime, in.getSchema());
		int[] selectColIdx = TableUtil.findColIndicesWithAssertAndHint(in.getColNames(), selectedColNames);

		TypeInformation timeColType = null;
		int timeColIdx = -1;
		if (timeColName != null) {
			timeColIdx = TableUtil.findColIndex(in.getColNames(), timeColName);
			if (timeColIdx != -1) {
				timeColType = in.getSchema().getFieldType(timeColName).get();
			}
		}

		String[] outColNames = new String[] {"starttime", "endtime", "colname", "quantile"};
		TypeInformation[] outColTypes = new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING, Types.STRING};

		QuantileWindowFunction windowFunc = new QuantileWindowFunction(selectedColNames, selectColIdx, qN, timeColIdx,
			timeInterval, timeColType);

		this.setOutputTable(DataStreamConversionUtil.toTable(getMLEnvironmentId(), in.getDataStream()
				.windowAll(SlidingProcessingTimeWindows.of(TimeUtil.convertTime(timeInterval + delayTime), TimeUtil.convertTime(timeInterval)))
				.apply(windowFunc),
			outColNames, outColTypes));

		return this;
	}

	private TypeInformation[] checkParameter(String[] selectedColNames, double timeInterval,
											 int N, String timeColName, int delayTime, TableSchema schema) {
		if (N <= 0 || N > 100000) {
			throw new RuntimeException("N must in (0, 100000].");
		}

		if (timeInterval < 0) {
			throw new RuntimeException("timeInterval must be larger than 0.");
		}

		TableUtil.assertNumericalCols(schema, selectedColNames);

		if (timeColName != null) {
			TypeInformation timeType = schema.getFieldType(timeColName).get();
			if (Types.LONG != timeType && Types.SQL_TIMESTAMP != timeType) {
				throw new RuntimeException("timeCol must be long or timestamp");
			}
		}

		if (delayTime < 0) {
			throw new RuntimeException("delayTime must be larger than 0.");
		}
		return TableUtil.findColTypes(schema, selectedColNames);
	}

}
