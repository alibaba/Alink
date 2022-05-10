package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkStream;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.FileProcFunction;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.AkSourceParams;

import java.io.IOException;

/**
 * Create a stream with a ak file from file system.
 */
@IoOpAnnotation(name = "ak", ioType = IOType.SourceStream)
@NameCn("AK文件数据源")
public final class AkSourceStreamOp extends BaseSourceStreamOp <AkSourceStreamOp>
	implements AkSourceParams <AkSourceStreamOp> {
	private static final long serialVersionUID = -1632712937397561402L;

	public AkSourceStreamOp() {
		this(new Params());
	}

	public AkSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(AkSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		final AkUtils.AkMeta meta;
		try {
			meta = AkUtils.getMetaFromPath(getFilePath());
		} catch (IOException e) {
			throw new IllegalArgumentException(
				"Could not get meta from ak file: " + getFilePath().getPathStr(), e
			);
		}

		String partitions = getPartitions();

		if (partitions == null) {
			return DataStreamConversionUtil.toTable(
				getMLEnvironmentId(),
				MLEnvironmentFactory
					.get(getMLEnvironmentId())
					.getStreamExecutionEnvironment()
					.createInput(new AkUtils.AkInputFormat(getFilePath(), meta))
					.name("AkSource")
					.rebalance(),
				TableUtil.schemaStr2Schema(meta.schemaStr)
			);
		} else {
			partitions = partitions.replace(",", " or ");

			final FilePath filePath = getFilePath();

			try {
				StreamOperator <?> selected = AkUtils
					.selectPartitionStreamOp(getMLEnvironmentId(), filePath, partitions);

				final String[] colNames = selected.getColNames();

				return DataStreamConversionUtil.toTable(
					getMLEnvironmentId(),
					selected
						.getDataStream()
						.flatMap(new FlatMapFunction <Row, Row>() {
							@Override
							public void flatMap(Row value, Collector <Row> out) throws Exception {
								Path path = filePath.getPath();

								for (int i = 0; i < value.getArity(); ++i) {
									path = new Path(path, String.format("%s=%s", colNames[i], value.getField(i)));
								}

								AkUtils.getFromFolderForEach(
									new FilePath(path, filePath.getFileSystem()),
									new FileProcFunction <FilePath, Boolean>() {
										@Override
										public Boolean apply(FilePath filePath) throws IOException {
											try (AkStream.AkReader akReader = new AkStream(filePath,
												meta).getReader()) {
												for (Row row : akReader) {
													out.collect(row);
												}
											}

											return true;
										}
									});
							}
						}),
					TableUtil.schemaStr2Schema(meta.schemaStr)
				);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}
}
