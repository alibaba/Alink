package com.alibaba.alink.operator.common.statistics.basicstat;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp.QIndex;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.params.statistics.HasRoundMode;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

public class QuantileWindowFunction implements AllWindowFunction <Row, Row, TimeWindow> {
	private static final long serialVersionUID = -3504832156252458350L;
	private String[] selectedColNames;
	private int[] selectedColIdx;
	private int quantileNum;
	private double timeInterval;
	private int timeColIdx;
	private TypeInformation timeColType;

	public QuantileWindowFunction(String[] selectedColNames, int[] selectedColIdx,
								  int quantileNum,
								  int timeColIdx, double timeInterval, TypeInformation timeColType) {
		this.selectedColNames = selectedColNames;
		this.selectedColIdx = selectedColIdx;
		this.quantileNum = quantileNum;
		this.timeInterval = timeInterval;
		this.timeColIdx = timeColIdx;
		this.timeColType = timeColType;
	}

	@Override
	public void apply(TimeWindow timeWindow, Iterable <Row> iterable, Collector <Row> collector) throws Exception {
		long startTime = timeWindow.getStart();
		long endTime = startTime + Math.round(timeInterval * 1000);

		//save data
		List <List <Object>> data = new ArrayList <>();
		int len = this.selectedColNames.length;
		for (int i = 0; i < len; i++) {
			data.add(new ArrayList <>());
		}
		Iterator <Row> iterator = iterable.iterator();
		if (timeColIdx < 0) {
			while (iterator.hasNext()) {
				Row row = iterator.next();
				for (int i = 0; i < len; i++) {
					data.get(i).add(row.getField(selectedColIdx[i]));
				}
			}
		} else {
			while (iterator.hasNext()) {
				Row row = iterator.next();
				long timestamp = getTime(row.getField(timeColIdx), timeColType);
				if (timestamp >= startTime && timestamp < endTime) {
					for (int i = 0; i < len; i++) {
						data.get(i).add(row.getField(selectedColIdx[i]));
					}
				}
			}
		}

		//sort
		int t = 0;
		for (List <Object> colData : data) {
			if (!colData.isEmpty()) {
				Collections.sort(colData, new SortUtils.ComparableComparator());
				QIndex QIndex = new QIndex(
					colData.size(),
					quantileNum,
					HasRoundMode.RoundMode.ROUND
				);

				Object[] quantileItems = new Object[quantileNum + 1];
				for (int i = 0; i <= quantileNum; i++) {
					quantileItems[i] = colData.get((int) QIndex.genIndex(i));
				}

				Row row = new Row(4);
				row.setField(0, timeStamp2Str(startTime));
				row.setField(1, timeStamp2Str(endTime));
				row.setField(2, selectedColNames[t]);
				row.setField(3, gson.toJson(quantileItems));

				collector.collect(row);
			}

			t++;
		}
	}

	private String timeStamp2Str(long timestamp) {
		DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		return sdf.format(timestamp);
	}

	private long getTime(Object val, TypeInformation type) {
		if (Types.LONG.getTypeClass().getName() == type.getTypeClass().getName()) {
			return (long) val;
		} else {
			return ((Timestamp) val).getTime();
		}
	}
}

