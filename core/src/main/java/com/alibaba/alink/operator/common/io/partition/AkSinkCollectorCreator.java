package com.alibaba.alink.operator.common.io.partition;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.io.filesystem.AkStream;
import com.alibaba.alink.common.io.filesystem.AkUtils.AkMeta;
import com.alibaba.alink.common.io.filesystem.FilePath;

import java.io.IOException;

public class AkSinkCollectorCreator implements SinkCollectorCreator {
	private final AkMeta akMeta;

	public AkSinkCollectorCreator(AkMeta akMeta) {
		this.akMeta = akMeta;
	}

	@Override
	public Collector <Row> createCollector(FilePath path) throws IOException {
		return new AkStream(path, akMeta).getWriter().getCollector();
	}
}
