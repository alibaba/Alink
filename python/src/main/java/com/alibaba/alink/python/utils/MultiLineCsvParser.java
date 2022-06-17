package com.alibaba.alink.python.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.io.csv.CsvParser;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Convert multi-line CSV content to a {@link MemSourceBatchOp} or a {@link MemSourceStreamOp}.
 */
@SuppressWarnings("unused")
public class MultiLineCsvParser {

	public static List <Row> csvToRows(String content, String schemaStr,
									   String lineTerminator, String fieldDelimiter, Character quoteChar) {
		TypeInformation <?>[] colTypes = TableUtil.getColTypes(schemaStr);
		CsvParser csvParser = new CsvParser(colTypes, fieldDelimiter, quoteChar);
		return Arrays.stream(content.split(lineTerminator))
			.map(csvParser::parse)
			.map(d -> d.f1)
			.map(Row::copy)
			.collect(Collectors.toList());
	}

	@SuppressWarnings("unused")
	public static MemSourceBatchOp csvToBatchOperator(String content, String schemaStr,
													  String lineTerminator, String fieldDelimiter,
													  Character quoteChar) {
		@SuppressWarnings("deprecation")
		TableSchema tableSchema = TableUtil.schemaStr2Schema(schemaStr);
		List <Row> rows = csvToRows(content, schemaStr, lineTerminator, fieldDelimiter, quoteChar);
		return new MemSourceBatchOp(rows, tableSchema);
	}

	@SuppressWarnings("unused")
	public static MemSourceStreamOp csvToStreamOperator(String content, String schemaStr,
														String lineTerminator, String fieldDelimiter,
														Character quoteChar) {
		@SuppressWarnings("deprecation")
		TableSchema tableSchema = TableUtil.schemaStr2Schema(schemaStr);
		List <Row> rows = csvToRows(content, schemaStr, lineTerminator, fieldDelimiter, quoteChar);
		return new MemSourceStreamOp(rows, tableSchema);
	}

	@SuppressWarnings("unused")
	public static MTable csvToMTable(String content, String schemaStr,
									 String lineTerminator, String fieldDelimiter,
									 Character quoteChar) {
		@SuppressWarnings("deprecation")
		TableSchema tableSchema = TableUtil.schemaStr2Schema(schemaStr);
		List <Row> rows = csvToRows(content, schemaStr, lineTerminator, fieldDelimiter, quoteChar);
		return new MTable(rows, tableSchema);
	}
}
