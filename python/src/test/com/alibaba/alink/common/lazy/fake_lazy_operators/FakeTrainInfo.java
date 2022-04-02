package com.alibaba.alink.common.lazy.fake_lazy_operators;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;

public class FakeTrainInfo implements Serializable {
	String content;

	public FakeTrainInfo(List <Row> rows) {
		content = rows.get(0).toString();
	}

	public String getContent() {
		return content;
	}

	@Override
	public String toString() {
		return "FakeTrainInfo{" +
			"content='" + content + '\'' +
			'}';
	}
}
