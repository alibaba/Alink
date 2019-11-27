package com.alibaba.alink.operator.common.optim.subfunc;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.model.ModelParamName;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Parse model rows to dense vector.
 *
 */
public class ParseRowModel extends RichMapPartitionFunction<Row, Tuple2<DenseVector, double[]>> {

    @Override
    public void mapPartition(Iterable<Row> iterable,
                             Collector<Tuple2<DenseVector, double[]>> collector) throws Exception {
        DenseVector coefVector = null;
        double[] lossCurve = null;
        int taskId = getRuntimeContext().getIndexOfThisSubtask();
        if (taskId == 0) {
            for (Row row : iterable) {
                Params params = Params.fromJson((String)row.getField(0));
                coefVector = params.get(ModelParamName.COEF);
                lossCurve = params.get(ModelParamName.LOSS_CURVE);
            }

            if (coefVector != null) {
                collector.collect(Tuple2.of(coefVector, lossCurve));
            }
        }
    }
}
