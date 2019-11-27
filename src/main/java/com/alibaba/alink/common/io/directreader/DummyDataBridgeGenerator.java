package com.alibaba.alink.common.io.directreader;

import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

@DataBridgeGeneratorPolicy(policy = "dummy")
public class DummyDataBridgeGenerator implements DataBridgeGenerator {
    @Override
    public DataBridge generate(BatchOperator batchOperator, Params globalParams) {
        return new DataBridge() {
            @Override
            public List<Row> read(FilterFunction<Row> filter) {
                return new ArrayList<>();
            }
        };
    }
}
