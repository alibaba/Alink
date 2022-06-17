package com.alibaba.alink.operator.common.statistics.basicstat;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.common.statistics.statistics.Summary2;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.params.statistics.HasStatLevel_L1;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yangxu
 */
public class StatOnTimeWindowAllByKey implements AllWindowFunction <Row, Map <String, SummaryResultTable>, TimeWindow> {

	private static final long serialVersionUID = -5715960707128472124L;
	public String[] values;
	private String[] colNames;
	private Class[] colTypes;
	private String[] statColNames;
	private String groupColName;

	public StatOnTimeWindowAllByKey(TableSchema schema, String groupColName, String[] values, String[] statColNames) {
		this.colNames = schema.getFieldNames();
		int n = this.colNames.length;
		this.colTypes = new Class[n];
		for (int i = 0; i < n; i++) {
			colTypes[i] = schema.getFieldTypes()[i].getTypeClass();
		}
		this.statColNames = statColNames;
		this.groupColName = groupColName;
		this.values = values;
	}

	@Override
	public void apply(TimeWindow window, Iterable <Row> values, Collector <Map <String, SummaryResultTable>> out)
		throws Exception {
		Map <String, SummaryResultTable> srtMap = new HashMap <>();
		for (int i = 0; i < this.values.length; i++) {
			String groupValue = this.values[i];
			WindowTable wt = new WindowTable(this.colNames, this.colTypes, values, this.groupColName, groupValue);
			SummaryResultTable srt = Summary2.streamSummary(wt, statColNames, 10,
				10, 100, 1000, HasStatLevel_L1.StatLevel.L3);

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(window.toString() + " \t " + String.valueOf(srt.col(0).count));
			}
			srtMap.put(this.values[i], srt);
		}
		out.collect(srtMap);
	}

}
