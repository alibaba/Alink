package com.alibaba.alink.common.sql.builtin.agg;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.utils.Functional.SerializableFunction;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * <timestamp, val>,  val can be number or vector.
 */
public class TimeSeriesAgg extends BaseUdaf <String, List <Tuple2 <Long, Object>>> {

	private static final String TIME_VECTOR_INNER_DELIMITER = "@";
	private static final String TIME_VECTOR_OUTER_DELIMITER = "&";

	private boolean dropLast;

	public TimeSeriesAgg(boolean dropLast) {
		this.dropLast = dropLast;
	}

	@Override
	public String getValue(List <Tuple2 <Long, Object>> values) {
		StringBuilder sbd = new StringBuilder();

		int end = dropLast ? values.size() - 1 : values.size();

		for (int i = 0; i < end; i++) {
			Tuple2 <Long, Object> value = values.get(i);
			sbd.append(TIME_VECTOR_OUTER_DELIMITER);
			sbd.append(value.f0)
				.append(TIME_VECTOR_INNER_DELIMITER);
			sbd.append(value.f1);
		}

		return sbd.length() == 0 ? "" : sbd.substring(1);
	}

	public static <T> List <Tuple2 <Long, T>> getValue(String seriesStr, SerializableFunction <String, T> parser) {
		if (seriesStr == null) {
			throw new RuntimeException("seriesStr is null.");
		}

		if (seriesStr.isEmpty()) {
			return new ArrayList <>();
		}

		List <Tuple2 <Long, T>> data = new ArrayList <>();

		String[] lines = seriesStr.split(TIME_VECTOR_OUTER_DELIMITER);
		for (String line : lines) {
			String[] lineValues = line.split(TIME_VECTOR_INNER_DELIMITER);
			if (lineValues.length != 2) {
				throw new RuntimeException("lineValue nums is larger than 2.");
			}
			data.add(
				Tuple2.of(Long.parseLong(lineValues[0]), parser.apply(lineValues[1]))
			);
		}

		data.sort(Comparator.comparing(o -> o.f0));

		return data;
	}

	@Override
	public void accumulate(List <Tuple2 <Long, Object>> acc, Object... values) {
		Object ds = values[0];
		long timestamp;
		if (ds instanceof Long) {
			timestamp = (long) ds;
		} else {
			timestamp = ((Timestamp) values[0]).getTime();
		}
		Object y = values[1];
		acc.add(Tuple2.of(timestamp, y));
	}

	@Override
	public List <Tuple2 <Long, Object>> createAccumulator() {
		return new ArrayList <Tuple2 <Long, Object>>();
	}

	@Override
	public void retract(List <Tuple2 <Long, Object>> lists, Object... values) {
		long timestamp;
		Object ds = values[0];
		if (values[0] instanceof Long) {
			timestamp = (long) ds;
		} else {
			timestamp = ((Timestamp) values[0]).getTime();
		}
		int index = lists.lastIndexOf(Tuple2.of(timestamp, values[1]));
		if (index >= 0) {
			lists.remove(index);
		}
	}

	@Override
	public void resetAccumulator(List <Tuple2 <Long, Object>> lists) {
		lists.clear();
	}

	@Override
	public void merge(List <Tuple2 <Long, Object>> lists, Iterable <List <Tuple2 <Long, Object>>> it) {
		for (List <Tuple2 <Long, Object>> data : it) {
			lists.addAll(data);
		}
	}
}
