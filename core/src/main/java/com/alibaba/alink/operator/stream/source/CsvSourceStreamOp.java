package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.csv.CsvTypeConverter;
import com.alibaba.alink.operator.common.io.csv.InternalCsvSourceStreamOp;
import com.alibaba.alink.operator.stream.StreamOperator;
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
@IoOpAnnotation(name = "csv", ioType = IOType.SourceStream)
@NameCn("CSV文件数据源")
public class CsvSourceStreamOp extends BaseSourceStreamOp <CsvSourceStreamOp>
	implements CsvSourceParams <CsvSourceStreamOp> {

	public CsvSourceStreamOp() {
		this(new Params());
	}

	public CsvSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(CsvSourceStreamOp.class), params);
	}

	public CsvSourceStreamOp(String filePath, String schemaStr) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, schemaStr)
		);
	}

	@Override
	protected Table initializeDataSource() {

		TableSchema schema = TableUtil.schemaStr2Schema(getSchemaStr());
		String[] colNames = schema.getFieldNames();
		TypeInformation <?>[] colTypes = schema.getFieldTypes();

		Params rawCsvParams = getParams().clone()
			.set(
				CsvSourceParams.SCHEMA_STR,
				TableUtil.schema2SchemaStr(new TableSchema(colNames, CsvTypeConverter.rewriteColTypes(colTypes)))
			);

		StreamOperator <?> source = new InternalCsvSourceStreamOp(rawCsvParams);

		source = CsvTypeConverter.toTensorPipelineModel(getParams(), colNames, colTypes).transform(source);
		source = CsvTypeConverter.toVectorPipelineModel(getParams(), colNames, colTypes).transform(source);
		source = CsvTypeConverter.toMTablePipelineModel(getParams(), colNames, colTypes).transform(source);

		return source.getOutputTable();
	}
}
