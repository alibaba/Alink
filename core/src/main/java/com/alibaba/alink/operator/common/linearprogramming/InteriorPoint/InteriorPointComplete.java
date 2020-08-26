package com.alibaba.alink.operator.common.linearprogramming.InteriorPoint;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.batch.linearprogramming.InteriorPointBatchOp;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

import static java.lang.Double.MAX_VALUE;
import static java.lang.Double.MIN_VALUE;

/**
 * Calculate the result of objective, multi coefficients and variables, then add or minus bounds.
 */
public class InteriorPointComplete extends CompleteResultFunction {
    @Override
    public List<Row> calc(ComContext context) {
        DenseVector x_hat = context.getObj(InteriorPointBatchOp.LOCAL_X_HAT);
        double[] c = context.getObj(InteriorPointBatchOp.STATIC_C);
        int n = context.getObj(InteriorPointBatchOp.N);
        double[][] bound = new double[3][n];
        Arrays.fill(bound[0], 0.0);
        Arrays.fill(bound[1], MAX_VALUE);//upper bound
        Arrays.fill(bound[2], MIN_VALUE);//lower bound
        List<Row> upperBoundsListRow = context.getObj(InteriorPointBatchOp.UPPER_BOUNDS);
        List<Row> lowerBoundsListRow = context.getObj(InteriorPointBatchOp.LOWER_BOUNDS);
        List<Row> unBoundsListRow = context.getObj(InteriorPointBatchOp.UN_BOUNDS);

        for (Row r : unBoundsListRow)
            bound[0][(int) r.getField(0)] = 1;
        for (Row r : upperBoundsListRow)
            bound[1][(int) r.getField(0)] = (double) r.getField(1);
        for (Row r : lowerBoundsListRow)
            bound[2][(int) r.getField(0)] = (double) r.getField(1);

        for (int i = 0; i < n; i++) {
            if (bound[0][i] == 0 && bound[1][i] == MAX_VALUE && bound[2][i] == MIN_VALUE)
                continue;
            if (bound[0][i] == 1)
                x_hat.set(i, x_hat.get(i) - x_hat.get(n + i));
            else if (bound[2][i] == MIN_VALUE)
                x_hat.set(i, bound[1][i] - x_hat.get(i));
            else
                x_hat.set(i, bound[2][i] + x_hat.get(i));
        }

        double result = 0.0;
        for (int i = 0; i < n; i++)
            result += x_hat.get(i) * c[i];

        Row row = new Row(1);
        row.setField(0, result);
        RowCollector collector = new RowCollector();
        collector.collect(row);
        return collector.getRows();
    }
}
