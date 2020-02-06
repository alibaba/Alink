package com.alibaba.alink.operator.common.io.kafka011;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.io.IOException;

public abstract class TupleRichOutputFormat extends RichOutputFormat<Tuple2<Boolean, Row>> {

    public TupleRichOutputFormat() {
    }

    public void open(int taskNumber, int numTasks) throws IOException {
    }

    public void writeRecord(Tuple2<Boolean, Row> cRow) throws IOException {
        if (cRow.f0) {
            this.writeAddRecord(Row.copy(cRow.f1));
        } else {
            this.writeDeleteRecord(Row.copy(cRow.f1));
        }

    }

    public abstract void writeAddRecord(Row var1) throws IOException;

    public abstract void writeDeleteRecord(Row var1) throws IOException;

    public abstract String getName();
}
