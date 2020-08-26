package com.alibaba.alink.operator.batch.linearprogramming;

import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.params.linearprogramming.LPParams;
import com.alibaba.alink.operator.common.linearprogramming.SimpleX.SimpleXCom;
import com.alibaba.alink.operator.common.linearprogramming.SimpleX.SimpleXComplete;
import com.alibaba.alink.operator.common.linearprogramming.SimpleX.SimpleXIterTermination;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * SimpleX method uses a full-tableau implementation of Dantzig's simplex algorithm [1]_
 *
 * References
 * ----------
 * .. [1] Dantzig, George B., Linear programming and extensions. Rand
 *        Corporation Research Study Princeton Univ. Press, Princeton, NJ,
 *        1963
 */
public class SimpleXBatchOp extends BatchOperator<SimpleXBatchOp> implements LPParams<SimpleXBatchOp> {
    public static final String TABLEAU = "tableau";
    public static final String UNBOUNDED = "unbounded";
    public static final String COMPLETED = "completed";
    public static final String M = "m";
    public static final String PIVOT_ROW_VALUE = "pivotRowValue"; //Tuple<Double, Integer, Integer, Vector> b/a, row index, task id, value of the pivot
    public static final String PIVOT_COL_INDEX = "pivotColIndex";
    public static final String PHASE = "phase";
    public static final String OBJECTIVE = "objective";
    public static final String PSEUDO_OBJECTIVE = "pseudoObjective";
    public static final String UPPER_BOUNDS = "upperBounds";
    public static final String LOWER_BOUNDS = "lowerBounds";
    public static final String UN_BOUNDS = "unBounds";

    static DataSet<Row> iterateICQ(DataSet<Row> Tableau,
                                   final int maxIter,
                                   DataSet<Row> Objective,
                                   DataSet<Row> UpperBounds,
                                   DataSet<Row> LowerBounds,
                                   DataSet<Row> UnBounds) {
        return new IterativeComQueue()
                .initWithBroadcastData(TABLEAU, Tableau)
                .initWithBroadcastData(OBJECTIVE, Objective)
                .initWithBroadcastData(UPPER_BOUNDS, UpperBounds)
                .initWithBroadcastData(LOWER_BOUNDS, LowerBounds)
                .initWithBroadcastData(UN_BOUNDS, UnBounds)
                .add(new SimpleXCom())
                .add(new AllReduce(PIVOT_ROW_VALUE, null,
                        new AllReduce.SerializableBiConsumer<double[], double[]>() {
                            @Override
                            public void accept(double[] a, double[] b) {
                                if (a[0] == -1.0 ||
                                        (b[0] > -1.0 && b[1] < a[1]) ||
                                        (b[1] == a[1] && b[0] < a[0])) {
                                    for (int i = 0; i < a.length; ++i)
                                        a[i] = b[i];
                                }
                            }
                        }))
                .setCompareCriterionOfNode0(new SimpleXIterTermination())
                .closeWith(new SimpleXComplete())
                .setMaxIter(maxIter)
                .exec();
    }

    @Override
    public SimpleXBatchOp linkFrom(BatchOperator<?>... inputs) {
        int inputLength = inputs.length;
        int maxIter = this.getMaxIter();
        if (inputLength <= 1) throw new AssertionError();
        DataSet<Row> TableauDataSet = inputs[0].getDataSet();
        DataSet<Row> CoefficientsDataSet = inputs[1].getDataSet();
        DataSet<Row> UpperBoundsDataSet = inputs[2].getDataSet();
        DataSet<Row> LowerBoundsDataSet = inputs[3].getDataSet();
        DataSet<Row> UnBoundsDataSet = inputs[4].getDataSet();

        DataSet<Row> Input = iterateICQ(
                TableauDataSet,
                maxIter,
                CoefficientsDataSet,
                UpperBoundsDataSet,
                LowerBoundsDataSet,
                UnBoundsDataSet)
                .map((MapFunction<Row, Row>) row -> row)
                .returns(new RowTypeInfo(Types.DOUBLE));
        this.setOutput(Input, new String[]{"MinObject"});

        return this;
    }
}
