package com.alibaba.alink.operator.common.linearprogramming.SimpleX;

import com.alibaba.alink.operator.batch.linearprogramming.SimpleXBatchOp;
import com.alibaba.alink.operator.common.linearprogramming.AppendArtificialVar;
import com.alibaba.alink.operator.common.linearprogramming.AppendSlack;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Double.MAX_VALUE;

/**
 * Minimize a linear objective function subject to linear equality and non-negativity
 * constraints using the two phase simplex method.
 */
public class SimpleXCom extends ComputeFunction {
    public static double[] invalidRow(int num) {
        double[] invalid_row = new double[num + 1];
        Arrays.fill(invalid_row, -1.0);
        return invalid_row;
    }

    @Override
    public void calc(ComContext context) {
        DenseVector objectiveRow = null;
        DenseVector pseudoObjectiveRow = null;
        List<Tuple2<Integer, DenseVector>> tableau = null;
        int phase = 1;
        int M = 0;
        int rowLength = 0;
        int nextPivotCol;
        int nextPivotRow;

        if (context.getStepNo() == 1) {
            /**
             * Initialize Tableau in first step
             * Apply pivot in other steps
             */
            List<Row> listTableau = context.getObj(SimpleXBatchOp.TABLEAU);
            List<Row> listCoefficients = context.getObj(SimpleXBatchOp.OBJECTIVE);
            List<Row> listUpperBounds = context.getObj(SimpleXBatchOp.UPPER_BOUNDS);
            List<Row> listLowerBounds = context.getObj(SimpleXBatchOp.LOWER_BOUNDS);
            List<Row> listUnBounds = context.getObj(SimpleXBatchOp.UN_BOUNDS);

            try {
                Tuple2<List<Tuple2<Integer, DenseVector>>, DenseVector> data0 = AppendSlack.append(listTableau, listCoefficients, listUpperBounds, listLowerBounds, listUnBounds);
                Tuple3<List<Tuple2<Integer, DenseVector>>, DenseVector, DenseVector> data1 = AppendArtificialVar.append(data0.f0, data0.f1);
                M = data0.f0.size();
                tableau = CustomPartitioner(data1.f0, context);
                objectiveRow = data1.f1;
                pseudoObjectiveRow = data1.f2;
                rowLength = objectiveRow.size();
                System.out.printf("task%d gets%d\n", context.getTaskId(), tableau.size());
            } catch (Exception e) {
                e.printStackTrace();
            }

            context.putObj(SimpleXBatchOp.PHASE, phase);
            context.putObj(SimpleXBatchOp.TABLEAU, tableau);
            context.putObj(SimpleXBatchOp.OBJECTIVE, objectiveRow);
            context.putObj(SimpleXBatchOp.PSEUDO_OBJECTIVE, pseudoObjectiveRow);
            context.putObj(SimpleXBatchOp.UNBOUNDED, false);
            context.putObj(SimpleXBatchOp.COMPLETED, false);
            context.putObj(SimpleXBatchOp.M, M);

        } else {
            tableau = context.getObj(SimpleXBatchOp.TABLEAU);
            phase = context.getObj(SimpleXBatchOp.PHASE);
            objectiveRow = context.getObj(SimpleXBatchOp.OBJECTIVE);
            pseudoObjectiveRow = context.getObj(SimpleXBatchOp.PSEUDO_OBJECTIVE);
            M = context.getObj(SimpleXBatchOp.M);
            int enteringVar = context.getObj(SimpleXBatchOp.PIVOT_COL_INDEX);
            double[] pivotRowList = context.getObj(SimpleXBatchOp.PIVOT_ROW_VALUE);
            int leavingVar = (int) pivotRowList[0];
            DenseVector pivotRow = list2vector(pivotRowList);
            rowLength = pivotRow.size();

            tableau = applyPivot(tableau, pivotRow, enteringVar, leavingVar);
            objectiveRow = applyPivot(objectiveRow, pivotRow, enteringVar);
            pseudoObjectiveRow = applyPivot(pseudoObjectiveRow, pivotRow, enteringVar);

            context.putObj(SimpleXBatchOp.TABLEAU, tableau);
            context.putObj(SimpleXBatchOp.OBJECTIVE, objectiveRow);
            context.putObj(SimpleXBatchOp.PSEUDO_OBJECTIVE, pseudoObjectiveRow);

            if (context.getTaskId() == 0)
                System.out.printf("step %d start leave %d enter %d, phase %d\ncurrent basis is\n", context.getStepNo(), leavingVar, enteringVar, phase);
            for (Tuple2<Integer, DenseVector> t : tableau)
                System.out.printf("x_%d = %.2f\n", t.f0, t.f1.get(0));
        }

        /**
         * Same leaving variable on different workers.
         */
        if (phase == 1)
            nextPivotCol = enterVariableSelection(pseudoObjectiveRow, false, 0, context);
        else
            nextPivotCol = enterVariableSelection(objectiveRow, false, M, context);
        if (nextPivotCol == -1 && phase == 1) {
            //phase 1 ends. phase 2 starts
            context.putObj(SimpleXBatchOp.PHASE, 2);
            nextPivotCol = enterVariableSelection(objectiveRow, false, M, context);
        }
        if (nextPivotCol != -1)
            context.putObj(SimpleXBatchOp.PIVOT_COL_INDEX, nextPivotCol);
        else
            context.putObj(SimpleXBatchOp.COMPLETED, true);
        /**
         * Different entering variable on different workers.
         */
        nextPivotRow = leaveVariableSelection(tableau, nextPivotCol, context);
        if (nextPivotRow == -1) {
            context.putObj(SimpleXBatchOp.UNBOUNDED, true);
            context.putObj(SimpleXBatchOp.PIVOT_ROW_VALUE, invalidRow(rowLength));
        } else {
            context.putObj(SimpleXBatchOp.PIVOT_ROW_VALUE,
                    vector2list(tableau.get(nextPivotRow), nextPivotCol));
        }
    }

    /**
     * determine the col for the pivot operation.
     *
     * @param objectiveRow  the minimizing negative element in objective row will be the entering variable
     * @param blade         If True, select the first column with a negative coefficient in the objective row.
     * @param reservedRange 0 for phase 1 and M for phase 2
     * @param context       context including worker id information
     * @return The index of the column of the pivot element
     */
    private int enterVariableSelection(DenseVector objectiveRow, Boolean blade, Integer reservedRange, ComContext context) {
        double[] rowData = objectiveRow.getData();
        int minColIndex = 0;
        double minColValue = 0;
        for (int i = 1; i < rowData.length - reservedRange; i++) {
            if (rowData[i] < 0 && Math.abs(rowData[i]) > 1e-6 && rowData[i] <= minColValue) {
                if (blade) {
                    return i - 1;
                }
                minColIndex = i;
                minColValue = rowData[i];
            }
        }
        return minColIndex - 1;
    }

    /**
     * determine the row for the pivot operation
     *
     * @param tableau  a 2D array representing the simplex tableau
     * @param pivotCol the index of entering variable
     * @param context  context including worker id information
     * @return the index of pivot row
     */
    private int leaveVariableSelection(List<Tuple2<Integer, DenseVector>> tableau, Integer pivotCol, ComContext context) {
        if (pivotCol == -1)
            return -1;
        int minRowIndex = -1;
        double minRowValue = MAX_VALUE;
        int i = 0;
        for (Tuple2<Integer, DenseVector> t : tableau) {
            DenseVector rowValue = t.f1;
            double q;
            if (rowValue.get(pivotCol + 1) > 0) {
                q = rowValue.get(0) / rowValue.get(pivotCol + 1);
                if (q < minRowValue && q > 0) {
                    minRowIndex = i;
                    minRowValue = q;
                }
            }
            i++;
        }
        return minRowIndex;
    }

    private double[] vector2list(Tuple2<Integer, DenseVector> row, Integer pivotCol) {
        if (pivotCol == -1)
            return invalidRow(row.f1.size());
        DenseVector scaledRow = row.f1.scale(1 / row.f1.get(pivotCol + 1));
        return scaledRow.prefix((double) row.f0).getData();
    }

    private DenseVector list2vector(double[] pivotRow) {
        DenseVector pivotRowValue = new DenseVector(pivotRow.length - 1);
        for (int i = 0; i < pivotRow.length - 1; i++)
            pivotRowValue.set(i, pivotRow[i + 1]);
        return pivotRowValue;
    }

    /**
     * Pivot the simplex tableau on the element given by (pivotRow, enteringVar).
     * The entering variable corresponds to the column given by enteringVal forcing
     * the variable basis[pivotRow] to leave the basis.
     *
     * @param tableau     A 2D array representing the simplex tableau
     * @param pivotRow    the row to update the tableau
     * @param enteringVar variable basis[enteringVar] will enter the basis
     * @param leavingVar  variable basis[leavingVar] will leave the basis
     * @return pivoted tableau
     */
    private List<Tuple2<Integer, DenseVector>> applyPivot(List<Tuple2<Integer, DenseVector>> tableau,
                                                          DenseVector pivotRow,
                                                          Integer enteringVar, Integer leavingVar) {
        for (Tuple2<Integer, DenseVector> t : tableau) {
            double c = t.f1.get(enteringVar + 1);
            if (!t.f0.equals(leavingVar) && c != 0) {
                t.f1.minusEqual(pivotRow.scale(c));
            } else if (t.f0.equals(leavingVar)) {
                t.f0 = enteringVar;
                t.f1 = pivotRow;
            }
        }
        return tableau;
    }

    /**
     * Pivot the objective on the element given by (pivotRow, enteringVal).
     *
     * @param objective   a 1D array representing the actual objective
     * @param pivotRow    the row to update objective
     * @param enteringVar variable basis[enteringVar] will enter basis
     * @return pivoted objective
     */
    private DenseVector applyPivot(DenseVector objective, DenseVector pivotRow,
                                   Integer enteringVar) {
        double c = objective.get(enteringVar + 1);
        objective.minusEqual(pivotRow.scale(c));
        return objective;
    }

    /**
     * distribute the tableau to different worker based on their task id
     *
     * @param data    the tableau to be distributed
     * @param context contest including id information
     * @return data owned by a certain worker
     */
    private static List<Tuple2<Integer, DenseVector>> CustomPartitioner(List<Tuple2<Integer, DenseVector>> data, ComContext context) {
        int taskId = context.getTaskId();
        int taskNum = context.getNumTask();
        List<Tuple2<Integer, DenseVector>> partitionData = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            if (i % taskNum == taskId) {
                partitionData.add(data.get(i));
            }
        }
        return partitionData;
    }
}