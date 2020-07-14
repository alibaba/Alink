package com.alibaba.alink.common.lazy.fake_lazy_operators;

import org.apache.flink.types.Row;

import java.util.List;

public class FakeModelInfo {
    String content;

    public FakeModelInfo(List<Row> rows) {
        content = rows.get(0).toString();
    }

    public String getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "FakeModelInfo{" +
                "content='" + content + '\'' +
                '}';
    }
}
