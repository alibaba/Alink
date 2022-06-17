package com.alibaba.alink.operator.common.statistics.basicstat;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.common.statistics.statistics.Summary2;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.params.statistics.HasStatLevel_L1;

/**
 * @author yangxu
 */
public class SetPartitionBasicStat implements MapPartitionFunction <Row, SummaryResultTable> {

	private static final long serialVersionUID = -5607403479996476267L;
	private String[] colNames;
	private Class[] colTypes;
	private HasStatLevel_L1.StatLevel statLevel;
	private String[] selectedColNames = null;

	public SetPartitionBasicStat(TableSchema schema) {
		this(schema, HasStatLevel_L1.StatLevel.L1);
	}

	/**
	 * @param schema
	 * @param statLevel: L1,L2,L3: 默认是L1
	 *                   L1 has basic statistic;
	 *                   L2 has simple statistic and cov/corr;
	 *                   L3 has simple statistic, cov/corr, histogram, freq, topk, bottomk;
	 */
	public SetPartitionBasicStat(TableSchema schema, HasStatLevel_L1.StatLevel statLevel) {
		this.colNames = schema.getFieldNames();
		int n = this.colNames.length;
		this.colTypes = new Class[n];
		for (int i = 0; i < n; i++) {
			colTypes[i] = schema.getFieldTypes()[i].getTypeClass();
		}
		this.statLevel = statLevel;
		this.selectedColNames = this.colNames;
	}

	/**
	 * @param schema
	 * @param statLevel:       L1,L2,L3: 默认是L1
	 *                         L1 has basic statistic;
	 *                         L2 has simple statistic and cov/corr;
	 *                         L3 has simple statistic, cov/corr, histogram, freq, topk, bottomk;
	 * @param selectedColNames
	 */
	public SetPartitionBasicStat(TableSchema schema, String[] selectedColNames, HasStatLevel_L1.StatLevel statLevel) {
		this.colNames = schema.getFieldNames();
		int n = this.colNames.length;
		this.colTypes = new Class[n];
		for (int i = 0; i < n; i++) {
			colTypes[i] = schema.getFieldTypes()[i].getTypeClass();
		}
		this.statLevel = statLevel;
		this.selectedColNames = selectedColNames;
	}

	@Override
	public void mapPartition(Iterable <Row> itrbl, Collector <SummaryResultTable> clctr) throws Exception {
		WindowTable wt = new WindowTable(this.colNames, this.colTypes, itrbl);
		SummaryResultTable srt = Summary2.batchSummary(wt, this.selectedColNames, 10, 10, 1000, 100, this.statLevel);
		if (srt != null) {
			clctr.collect(srt);
		}
	}

}
