package com.alibaba.alink.operator.common.io.partition;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.io.filesystem.AkStream;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.AkMeta;
import com.alibaba.alink.common.io.filesystem.AkUtils.FileProcFunction;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.TableUtil;

import java.io.IOException;

public class AkSourceCollectorCreator implements SourceCollectorCreator {
	private final AkMeta akMeta;

	public AkSourceCollectorCreator(AkMeta akMeta) {
		this.akMeta = akMeta;
	}

	@Override
	public TableSchema schema() {
		return TableUtil.schemaStr2Schema(akMeta.schemaStr);
	}

	@Override
	public void collect(FilePath filePath, Collector <Row> collector) throws IOException {
		AkUtils.getFromFolderForEach(
			filePath,
			new FileProcFunction <FilePath, Boolean>() {
				@Override
				public Boolean apply(FilePath filePath) throws IOException {
					try (AkStream.AkReader akReader = new AkStream(filePath, akMeta).getReader()) {
						for (Row row : akReader) {
							collector.collect(row);
						}
					}

					return true;
				}
			});
	}
}
