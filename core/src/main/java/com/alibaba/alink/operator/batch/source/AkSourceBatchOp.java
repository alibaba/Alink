package com.alibaba.alink.operator.batch.source;

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
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.AkSourceParams;

import java.io.IOException;

/**
 * Read an ak file from file system.
 */
@IoOpAnnotation(name = "ak", ioType = IOType.SourceBatch)
@NameCn("AK文件读入")
public final class AkSourceBatchOp extends BaseSourceBatchOp <AkSourceBatchOp>
	implements AkSourceParams <AkSourceBatchOp> {

	private static final long serialVersionUID = 7493386303148970332L;

	public AkSourceBatchOp() {
		this(new Params());
	}

	public AkSourceBatchOp(Params params) {
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
			return DataSetConversionUtil.toTable(
				getMLEnvironmentId(),
				MLEnvironmentFactory
					.get(getMLEnvironmentId())
					.getExecutionEnvironment()
					.createInput(new AkUtils.AkInputFormat(getFilePath(), meta))
					.name("AkSource")
					.rebalance(),
				TableUtil.schemaStr2Schema(meta.schemaStr)
			);
		} else {
			partitions = partitions.replace(",", " or ");

			final FilePath filePath = getFilePath();

			try {
				BatchOperator <?> selected = AkUtils
					.selectPartitionBatchOp(getMLEnvironmentId(), filePath, partitions);

				final String[] colNames = selected.getColNames();

				return DataSetConversionUtil.toTable(
					getMLEnvironmentId(),
					selected
						.getDataSet()
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
