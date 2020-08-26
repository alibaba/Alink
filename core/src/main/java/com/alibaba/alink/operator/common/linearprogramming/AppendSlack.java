package com.alibaba.alink.operator.common.linearprogramming;

import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class AppendSlack {
    /**
     * Given a linear programming problem of the form:
     * Minimize:
     * c @ x
     * Subject to:
     * A_ub @ x <= b_ub
     * A_eq @ x == b_eq
     * lb <= x <= ub
     * where lb = 0 and ub = None when upperBounds/lowerBounds is null
     * Return the problem in standard form:
     * Minimize:
     * c @ x
     * Subject to::
     * A @ x == b
     * x >= 0
     * by adding slack variables and making variable substitutions as necessary.
     *
     * @param constraints  The value of constraints, expected form of Row is "a_i1 a_i2 ... a_in [le,ge,eq] b_i".
     * @param coefficients Coefficients of the linear objective function to be minimized.
     * @param upperBounds  SparseVector for (x_id, max) pairs
     * @param lowerBounds  SparseVector for (x_id, min) pairs
     * @param unbounded    list of independent variables
     * @return tableau, objective row
     */

    public static Tuple2<List<Tuple2<Integer, DenseVector>>, DenseVector> append(
            List<Row> constraints,
            List<Row> coefficients,
            List<Row> upperBounds,  // can be null
            List<Row> lowerBounds,  // can be null
            List<Row> unbounded     // can be null
    ) throws Exception {
        /** constraints     : Row of (Double, Double, ...... , String, Double)
         *  coefficients    : Row of (int, Double)
         *  upperBounds     : Row of (int, Double)
         *  lowerBounds     : Row of (int, Double)
         *  unbounded       : Row of (int)
         *
         *  return Tableau with slack variables, coefficients with slack variables
         * */
        int varNum = constraints.get(0).getArity() - 2;
        int freeNUm = unbounded == null ? 0 : unbounded.size();
        ArrayList<Tuple2<Integer, DenseVector>> tableau = new ArrayList<>();
        int mCount = 0;

        for (Row row : constraints) {
            String relation = (String) row.getField(varNum);
            DenseVector constraint = DenseVector.zeros(varNum + freeNUm + 1);
            // set constraint
            for (int i = 1; i <= varNum; i++)
                constraint.set(i, (double) row.getField(i - 1));
            // set RHS
            constraint.set(0, (double) row.getField(varNum + 1));
            // unbounded: substitute xi = xi+ + xi-
            for (int i = 0; i < freeNUm; i++)
                constraint.set(varNum + i + 1,
                        -constraint.get(1 + (Integer) unbounded.get(i).getField(0))
                );
            // lowerBound: substitute xi = xi - bound
            if (lowerBounds != null) {
                for (Row lowerBound : lowerBounds) {
                    int i = (Integer) lowerBound.getField(0);
                    double bound = (Double) lowerBound.getField(1);
                    double a_ti = constraint.get(1 + i);
                    double b_t = constraint.get(0);
                    constraint.set(0, b_t - bound * a_ti);
                }
            }
            // assess relation
            switch (relation) {
                case "le":
                    tableau.add(new Tuple2<>(varNum + freeNUm + mCount, constraint));
                    mCount++;
                    break;
                case "ge":
                    // a @ x >= b : substitute xi = -xi'
                    tableau.add(new Tuple2<>(varNum + freeNUm + mCount, constraint.scale(-1)));
                    mCount++;
                    break;
                case "eq":
                    // a @ x == b : substitute a @ x <= b && a @ x >= b
                    tableau.add(new Tuple2<>(varNum + freeNUm + mCount, constraint));
                    tableau.add(new Tuple2<>(varNum + freeNUm + mCount + 1, constraint.scale(-1)));
                    mCount += 2;
                    break;
                default:
                    System.out.printf("unexpected relation %s\n", relation);
                    break;
            }
        }

        if (upperBounds != null) {
            for (Row upperBound : upperBounds) {
                DenseVector constraint = DenseVector.zeros(varNum + freeNUm + 1);
                int i = (Integer) upperBound.getField(0);
                double bound = (Double) upperBound.getField(1);
                constraint.set(1 + i, 1);
                constraint.set(0, bound);
                if (lowerBounds != null) {
                    for (Row lowerBound : lowerBounds) {
                        if (i == (Integer) lowerBound.getField(0)) {
                            constraint.set(0, bound - (Double) lowerBound.getField(1));
                        }
                    }
                }
                tableau.add(new Tuple2<>(varNum + freeNUm + mCount, constraint));
                mCount++;
            }
        }

        //add slack on tableau
        for (int i = 0; i < mCount; i++) {
            DenseVector d = tableau.get(i).f1.concatenate(new DenseVector(mCount));
            d.set(varNum + freeNUm + i + 1, 1.0);
            int basisId = tableau.get(i).f0;
            tableau.set(i, new Tuple2<>(basisId, d));
        }

        //initialize object
        DenseVector object = DenseVector.zeros(1 + varNum + freeNUm + mCount);
        for (Row coefficient : coefficients) {
            int idx = (Integer) coefficient.getField(0);
            double c = (Double) coefficient.getField(1);
            object.set(idx + 1, c);
        }

        // add unbounded xi on object
        for (int i = 0; i < freeNUm; i++)
            object.set(varNum + i + 1, -object.get(1 + (Integer) unbounded.get(i).getField(0)));

        // lowerBound: substitute xi = xi - bound
        if (lowerBounds != null) {
            for (Row lowerBound : lowerBounds) {
                int i = (Integer) lowerBound.getField(0);
                double bound = (Double) lowerBound.getField(1);
                double a_ti = object.get(1 + i);
                double b_t = object.get(0);
                object.set(0, b_t - bound * a_ti);
            }
        }

        return new Tuple2<>(tableau, object);
    }

    public static void main(String[] args) throws Exception {
        ArrayList<Row> r2 = new ArrayList<>();
        r2.add(Row.of(0, -1.0));
        r2.add(Row.of(1, 4.0));

        ArrayList<Row> r1 = new ArrayList<>();
        r1.add(Row.of(-3.0, 1.0, "le", 6.0));
        r1.add(Row.of(1.0, 2.0, "le", 4.0));

        ArrayList<Row> r3 = new ArrayList<>();
        r3.add(Row.of(1, -3.0));

        ArrayList<Row> r5 = new ArrayList<>();
        r5.add(Row.of(1, 300.0));

        ArrayList<Row> r4 = new ArrayList<>();
        r4.add(Row.of(0));
        Tuple2<List<Tuple2<Integer, DenseVector>>, DenseVector> data = append(r1, r2, r5, r3, r4);
        Tuple3<List<Tuple2<Integer, DenseVector>>, DenseVector, DenseVector> data2 = AppendArtificialVar.append(data.f0, data.f1);
        for (Tuple2<Integer, DenseVector> t : data2.f0) {
            System.out.printf("%d:", t.f0);
            LinearProgrammingUtil.LPPrintVector(t.f1);
        }
        LinearProgrammingUtil.LPPrintVector(data2.f1);
        LinearProgrammingUtil.LPPrintVector(data2.f2);
    }
}
