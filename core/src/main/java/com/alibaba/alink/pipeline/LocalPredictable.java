package com.alibaba.alink.pipeline;

import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import org.apache.flink.table.api.TableSchema;

/**
 * LocalPredictable get a {@link LocalPredictor} using {@link TableSchema}
 * or string representation of {@link TableSchema}.
 */
public interface LocalPredictable {

	LocalPredictor getLocalPredictor(TableSchema inputSchema) throws Exception;

	default LocalPredictor getLocalPredictor(String inputSchemaStr) throws Exception {
		return getLocalPredictor(CsvUtil.schemaStr2Schema(inputSchemaStr));
	}

}
