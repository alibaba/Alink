package com.alibaba.alink.common.io.directreader;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A DataBridge which read data from memory.
 */
public class MemoryDataBridge implements DataBridge {
	List <Row> rows;

	public MemoryDataBridge(List <Row> rows) {
		this.rows = rows;
	}

	@Override
	public List <Row> read(FilterFunction <Row> filter) {
		if (filter == null) {
			return rows;
		}

		return rows.stream()
			.filter(x -> {
				try {
					return filter.filter(x);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			})
			.collect(Collectors.toList());
	}
}
