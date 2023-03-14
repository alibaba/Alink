package com.alibaba.alink.common.io.xls;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

public interface XlsFile {
	Tuple2 <RichInputFormat <Row, FileInputSplit>, TableSchema> createInputFormat(Params params);

	FileOutputFormat createOutputFormat(Params params, TableSchema schema);
}
