package com.alibaba.alink.operator.local.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.csv.CsvTypeConverter;
import com.alibaba.alink.operator.common.io.csv.InternalCsvSourceBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.CsvSourceParams;

/**
 * Data source of a CSV (Comma Separated Values) file.
 * <p>
 * The file can reside in places including:
 * <p><ul>
 * <li> local file system
 * <li> hdfs
 * <li> http
 * </ul></p>
 */
@NameCn("CSV文件读入")
public class CsvSourceLocalOp extends BaseSourceLocalOp <CsvSourceLocalOp>
	implements CsvSourceParams <CsvSourceLocalOp> {

	public CsvSourceLocalOp() {
		this(new Params());
	}

	public CsvSourceLocalOp(Params params) {
		super(params);
	}

	public CsvSourceLocalOp(String filePath, String schemaStr) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, schemaStr)
		);
	}

	public CsvSourceLocalOp(String filePath, TableSchema schema) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, TableUtil.schema2SchemaStr(schema))
		);
	}

	public CsvSourceLocalOp(String filePath, String[] colNames, TypeInformation <?>[] colTypes,
							String fieldDelim, String rowDelim) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, TableUtil.schema2SchemaStr(new TableSchema(colNames, colTypes)))
			.set(FIELD_DELIMITER, fieldDelim)
			.set(ROW_DELIMITER, rowDelim)
		);
	}

	@Override
	protected MTable initializeDataSource() {
		TableSchema schema = TableUtil.schemaStr2Schema(getSchemaStr());
		String[] colNames = schema.getFieldNames();
		TypeInformation <?>[] colTypes = schema.getFieldTypes();

		Params rawCsvParams = getParams().clone()
			.set(
				CsvSourceParams.SCHEMA_STR,
				TableUtil.schema2SchemaStr(new TableSchema(colNames, CsvTypeConverter.rewriteColTypes(colTypes)))
			);

		LocalOperator <?> source = new InternalCsvSourceLocalOp(rawCsvParams);

		source = CsvTypeConverter.toTensorPipelineModel(getParams(), colNames, colTypes).transform(source);
		source = CsvTypeConverter.toVectorPipelineModel(getParams(), colNames, colTypes).transform(source);
		source = CsvTypeConverter.toMTablePipelineModel(getParams(), colNames, colTypes).transform(source);

		return source.getOutputTable();
	}
}
