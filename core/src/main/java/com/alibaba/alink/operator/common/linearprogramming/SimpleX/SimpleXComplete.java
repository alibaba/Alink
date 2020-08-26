package com.alibaba.alink.operator.common.linearprogramming.SimpleX;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.batch.linearprogramming.SimpleXBatchOp;
import com.alibaba.alink.operator.common.linearprogramming.LinearProgrammingUtil;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Calculate the result, result is the first element of objective row.
 */
public class SimpleXComplete extends CompleteResultFunction {
    @Override
    public List<Row> calc(ComContext context) {
        DenseVector object = context.getObj(SimpleXBatchOp.OBJECTIVE);
        //LinearProgrammingUtil.LPPrintVector(object);
        Row row = new Row(1);
        row.setField(0, -1 * object.get(0));
        RowCollector collector = new RowCollector();
        collector.collect(row);
        return collector.getRows();
    }
}
