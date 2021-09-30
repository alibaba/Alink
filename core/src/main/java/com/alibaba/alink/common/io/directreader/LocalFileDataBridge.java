package com.alibaba.alink.common.io.directreader;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.io.csv.CsvParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class LocalFileDataBridge implements DataBridge {

	private final String path;
	private final int size;
	private final TypeInformation <?>[] colTypes;

	public LocalFileDataBridge(String path, int size, TypeInformation <?>[] colTypes) {
		this.path = path;
		this.size = size;
		this.colTypes = colTypes;
	}

	@Override
	public List <Row> read(FilterFunction <Row> filter) {
		List <Row> rows = new ArrayList <>(size);
		CsvParser parser = new CsvParser(colTypes, ",", '"');
		try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
			for (int i = 0; i < size; i += 1) {
				String line = reader.readLine();
				Tuple2 <Boolean, Row> tuple2 = parser.parse(line);
				if (!tuple2.f0) {
					throw new RuntimeException("Failed to parse line: " + line);
				}
				if (null == filter || filter.filter(tuple2.f1)) {
					rows.add(tuple2.f1);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return rows;
	}
}
