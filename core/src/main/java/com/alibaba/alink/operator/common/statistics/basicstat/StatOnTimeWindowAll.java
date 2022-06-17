package com.alibaba.alink.operator.common.statistics.basicstat;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.statistics.statistics.Summary2;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.params.statistics.HasStatLevel_L1.StatLevel;

/**
 * @author yangxu
 */
public class StatOnTimeWindowAll implements AllWindowFunction <Row, SummaryResultTable, TimeWindow> {

	private static final long serialVersionUID = 3329375326702908192L;
	private String[] colNames;
	private Class[] colTypes;
	private String[] statColNames;
	private Class[] statColTypes;
	private StatLevel statLevel;
	/**
	 * ouput or not when window is empty.
	 */
	private Boolean allowEmptyOutput = false;

	public StatOnTimeWindowAll(TableSchema schema) {
		this(schema, null, StatLevel.L1);
	}

	public StatOnTimeWindowAll(TableSchema schema, String[] statColNames) {
		this(schema, statColNames, StatLevel.L3);
	}

	public StatOnTimeWindowAll(TableSchema schema, String[] statColNames, StatLevel statLevel) {
		this.colNames = schema.getFieldNames();
		int n = this.colNames.length;
		this.colTypes = new Class[n];
		for (int i = 0; i < n; i++) {
			colTypes[i] = schema.getFieldTypes()[i].getTypeClass();
		}
		if ((statColNames != null) && (statColNames.length > 0)) {
			this.statColNames = statColNames;
		} else {
			this.statColNames = this.colNames;
		}
		TypeInformation[] selectedTypes = TableUtil.findColTypesWithAssertAndHint(schema, this.statColNames);
		n = this.statColNames.length;
		this.statColTypes = new Class[n];
		for (int i = 0; i < n; ++i) {
			this.statColTypes[i] = selectedTypes[i].getTypeClass();
		}
		this.statLevel = statLevel;
	}

	public StatOnTimeWindowAll(TableSchema schema, String[] statColNames, StatLevel statLevel,
							   Boolean allowEmptyOutput) {
		this(schema, statColNames, statLevel);
		this.allowEmptyOutput = allowEmptyOutput;
	}

	@Override
	public void apply(TimeWindow window, Iterable <Row> values, Collector <SummaryResultTable> out) throws Exception {
		if ((values != null) && (values.iterator().hasNext())) {
			WindowTable wt = new WindowTable(this.colNames, this.colTypes, values);
			SummaryResultTable srt = Summary2.streamSummary(wt, statColNames,
				10, 10, 100, 10, statLevel);
			out.collect(srt);
		} else {
			if (this.allowEmptyOutput) {
				out.collect(
					new SummaryResultTable(
						this.statColNames,
						this.statColTypes,
						this.statLevel
					)
				);
			}
		}
	}
}
