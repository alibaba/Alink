package com.alibaba.alink.common.utils;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Collector of Row type data.
 */
public class RowCollector implements Collector<Row> {
	private List<Row> rows;

	public RowCollector() {
		this(null);
	}

	public RowCollector(List<Row> rows) {
		this.rows = rows;
		if (null == this.rows) {
			this.rows = new ArrayList<>();
		}
	}

	/**
	 * Get the collected rows.
	 *
	 * @return list of the collected rows
	 */
	public List<Row> getRows() {
		return rows;
	}

	@Override
	public void collect(Row row) {
		rows.add(row);
	}

	@Override
	public void close() {
		rows.clear();
	}

	/**
	 * Removes all of the rows from this collector.
	 */
	public void clear() {
		rows.clear();
	}
}
