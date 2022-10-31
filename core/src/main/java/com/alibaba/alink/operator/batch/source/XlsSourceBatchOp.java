package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.plugin.wrapper.RichInputFormatGenericWithClassLoader;
import com.alibaba.alink.common.io.xls.XlsReaderClassLoader;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.params.io.XlsSourceParams;

@IoOpAnnotation(name = "xls", ioType = IOType.SourceBatch)
@NameCn("Xls和Xlsx表格读入")
public class XlsSourceBatchOp extends BaseSourceBatchOp <XlsSourceBatchOp>
	implements XlsSourceParams <XlsSourceBatchOp> {
	public XlsSourceBatchOp() {
		this(new Params());
	}

	private final XlsReaderClassLoader factory;

	public XlsSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(XlsSourceBatchOp.class), params);
		factory = new XlsReaderClassLoader("0.11");
	}

	@Override
	protected Table initializeDataSource() {

		Tuple2 <RichInputFormat <Row, FileInputSplit>, TableSchema> sourceFunction = XlsReaderClassLoader
			.create(factory)
			.create(getParams());

		RichInputFormat <Row, InputSplit> inputFormat
			= new RichInputFormatGenericWithClassLoader <>(factory, sourceFunction.f0);

		DataSet data = MLEnvironmentFactory
			.get(getMLEnvironmentId())
			.getExecutionEnvironment()
			.createInput(inputFormat, new RowTypeInfo(sourceFunction.f1.getFieldTypes()));

		return DataSetConversionUtil.toTable(getMLEnvironmentId(), data, sourceFunction.f1);
	}
}
