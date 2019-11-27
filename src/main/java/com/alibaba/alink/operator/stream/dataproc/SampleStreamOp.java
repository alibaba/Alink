package com.alibaba.alink.operator.stream.dataproc;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dataproc.SampleParams;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Random;

/**
 * Sample with given ratio with or without replacement.
 */
@SuppressWarnings("uncheck")
public class SampleStreamOp extends StreamOperator<SampleStreamOp> implements SampleParams<SampleStreamOp> {

    public SampleStreamOp() {
        this(new Params());
    }

    public SampleStreamOp(double ratio) {
        this(new Params().set(RATIO, ratio));
    }

    public SampleStreamOp(Params params) {
        super(params);
    }

    @Override
    public SampleStreamOp linkFrom(StreamOperator<?>... inputs) {
        StreamOperator<?> in = checkAndGetFirst(inputs);
        final double ratio = getRatio();
        Preconditions.checkArgument(ratio >= 0. && ratio <= 1.);

        DataStream<Row> rows = in.getDataStream()
            .flatMap(new RichFlatMapFunction<Row, Row>() {
                transient Random random;

                @Override
                public void open(Configuration parameters) throws Exception {
                    random = new Random();
                }

                @Override
                public void flatMap(Row value, Collector<Row> out) throws Exception {
                    if (random.nextDouble() <= ratio) {
                        out.collect(value);
                    }
                }
            });

        this.setOutput(rows, in.getSchema());
        return this;
    }
}
