package com.alibaba.alink.operator.stream.source;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.stream.StreamOperator;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import static java.lang.Thread.sleep;

/**
 * Stream sources that represents a range of integers.
 */
public final class NumSeqSourceStreamOp extends StreamOperator<NumSeqSourceStreamOp> {

    public NumSeqSourceStreamOp(long n) {
        this(1L, n);
    }

    public NumSeqSourceStreamOp(long from, long to) {
        this(from, to, new Params());
    }

    public NumSeqSourceStreamOp(long from, long to, Params params) {
        this(from, to, "num", params);
    }

    public NumSeqSourceStreamOp(long from, long to, String colName) {
        this(from, to, colName, new Params());
    }

    public NumSeqSourceStreamOp(long from, long to, String colName, Params params) {
        super(params);
        DataStreamSource<Long> seq = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment().generateSequence(from, to);
        this.setOutputTable(MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment().fromDataStream(seq, colName));
    }

    public NumSeqSourceStreamOp(long from, long to, double timePerSample) {
        this(from, to, timePerSample, null);
    }

    public NumSeqSourceStreamOp(long from, long to, double timePerSample, Params params) {
        this(from, to, "num", timePerSample, params);
    }

    public NumSeqSourceStreamOp(long from, long to, String colName, double timePerSample) {
        this(from, to, colName, timePerSample, null);
    }

    public NumSeqSourceStreamOp(long from, long to, String colName, double timePerSample, Params params) {
        super(params);

        DataStreamSource<Long> seq = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment().generateSequence(from, to);
        DataStream<Long> data = seq.map(new transform(new Double[]{timePerSample}));

        this.setOutputTable(MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment().fromDataStream(data, colName));
    }

    public NumSeqSourceStreamOp(long from, long to, Double[] timeZones) {
        this(from, to, "num", timeZones, null);
    }

    public NumSeqSourceStreamOp(long from, long to, Double[] timeZones, Params params) {
        this(from, to, "num", timeZones, params);
    }

    public NumSeqSourceStreamOp(long from, long to, String colName, Double[] timeZones) {
        this(from, to, colName, timeZones, null);
    }

    public NumSeqSourceStreamOp(long from, long to, String colName, Double[] timeZones, Params params) {
        super(params);

        DataStreamSource<Long> seq = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment().generateSequence(from, to);
        DataStream<Long> data = seq.map(new transform(timeZones));

        this.setOutputTable(MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment().fromDataStream(data, colName));
    }

    @Override
    public NumSeqSourceStreamOp linkFrom(StreamOperator<?>... inputs) {
        throw new UnsupportedOperationException(
            "Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public class transform extends AbstractRichFunction
        implements MapFunction<Long, Long> {

        RandomDataGenerator rd = new RandomDataGenerator();

        boolean update_seed = false;
        int numWorker;
        private Double timePerSample;
        private Double[] timeZones;

        public transform(Double[] timeZones) {
            if (timeZones.length == 1) {
                this.timePerSample = timeZones[0];
            } else {
                this.timeZones = timeZones;
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.numWorker = getRuntimeContext().getNumberOfParallelSubtasks();
        }

        @Override
        public Long map(Long value) throws Exception {

            if (!update_seed) {
                rd.reSeed(value);
                update_seed = true;
            }

            Long sleep_param;
            if (timeZones == null) {
                sleep_param = Math.round(1000 * timePerSample * this.numWorker);
            } else if (timeZones.length == 2) {
                sleep_param = Math.round(1000 * (timeZones[0] + 0.01 * rd.nextInt(0, 100) * (timeZones[1] - timeZones[0]))
                    * this.numWorker);
            } else {
                throw (new RuntimeException("time parameter is wrong!"));
            }
            sleep(sleep_param);
            return value;
        }
    }

}
