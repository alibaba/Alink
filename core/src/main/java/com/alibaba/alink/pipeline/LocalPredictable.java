package com.alibaba.alink.pipeline;

import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.operator.common.io.csv.CsvUtil;

/**
 * LocalPredictable get a {@link LocalPredictor} using {@link TableSchema}
 * or string representation of {@link TableSchema}.
 */
public interface LocalPredictable {

	LocalPredictor collectLocalPredictor(TableSchema inputSchema) throws Exception;

	default LocalPredictor collectLocalPredictor(String inputSchemaStr) throws Exception {
		return collectLocalPredictor(CsvUtil.schemaStr2Schema(inputSchemaStr));
	}

	@Deprecated
	default LocalPredictor getLocalPredictor(TableSchema inputSchema) throws Exception {
		return collectLocalPredictor(inputSchema);
	}

	@Deprecated
	default LocalPredictor getLocalPredictor(String inputSchemaStr) throws Exception {
		return collectLocalPredictor(inputSchemaStr);
	}

}
