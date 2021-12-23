package com.alibaba.alink.common.io.directreader;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class LocalFileDataBridge implements DataBridge {

	private final String path;

	public LocalFileDataBridge(String path) {
		this.path = path;
	}

	@Override
	public List <Row> read(FilterFunction <Row> filter) {
		try {
			Tuple2 <TableSchema, List <Row>> schemaAndRows = AkUtils.readFromPath(new FilePath(path));
			if (null == filter) {
				return schemaAndRows.f1;
			}
			return schemaAndRows.f1.stream()
				.filter(x -> {
					try {
						return filter.filter(x);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				})
				.collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
