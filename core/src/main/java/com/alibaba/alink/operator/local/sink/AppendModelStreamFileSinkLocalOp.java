package com.alibaba.alink.operator.local.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.stream.model.FileModelStreamSink;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.AppendModelStreamFileSinkParams;

import java.io.IOException;
import java.sql.Timestamp;

@InputPorts(values = {@PortSpec(PortType.MODEL)})
@NameCn("模型流导出")
public class AppendModelStreamFileSinkLocalOp extends BaseSinkLocalOp <AppendModelStreamFileSinkLocalOp>
	implements AppendModelStreamFileSinkParams <AppendModelStreamFileSinkLocalOp> {

	public AppendModelStreamFileSinkLocalOp() {
		this(new Params());
	}

	public AppendModelStreamFileSinkLocalOp(Params params) {
		super(params);
	}

	@Override
	protected AppendModelStreamFileSinkLocalOp sinkFrom(LocalOperator <?> in) {
		final FilePath filePath = getFilePath();
		final Timestamp timestamp = ModelStreamUtils.createStartTime(getModelTime());
		final int numFiles = 1; // todo, support getNumFiles();
		final int numKeepModel = getNumKeepModel();
		final TableSchema schema = in.getSchema();

		final FileModelStreamSink sink = new FileModelStreamSink(
			filePath, TableUtil.schema2SchemaStr(schema)
		);

		try {
			sink.initializeGlobal();
			sink.open(timestamp, 0);
			for (Row value : in.getOutputTable().getRows()) {
				sink.collect(value);
			}
			sink.close();
			sink.finalizeGlobal(timestamp, in.getOutputTable().getNumRow(), numFiles, numKeepModel);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException("Error. ", e);
		}

		return this;
	}
}
