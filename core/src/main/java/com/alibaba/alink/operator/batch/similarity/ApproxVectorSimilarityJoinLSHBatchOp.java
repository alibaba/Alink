package com.alibaba.alink.operator.batch.similarity;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.BaseLSH;
import com.alibaba.alink.operator.common.feature.LocalitySensitiveHashApproxFunctions;
import com.alibaba.alink.params.similarity.ApproxVectorJoinLSHParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import static com.alibaba.alink.operator.batch.similarity.ApproxVectorSimilarityTopNLSHBatchOp.DISTANCE_COL;

/**
 * ApproxVectorSimilarityJoinLSHBatchOp is used to join two vectors whose distance is below threshold from two datasets
 * separately. It's an approximate method using LSH.
 * <p>
 * The two datasets must each contain at least two columns: vector column and id column.
 * <p>
 * The class supports two distance type: EUCLIDEAND and JACCARD.
 * <p>
 * The output contains three columns: leftId, rightId, distance.
 *
 */
public final class ApproxVectorSimilarityJoinLSHBatchOp extends BatchOperator<ApproxVectorSimilarityJoinLSHBatchOp>
    implements ApproxVectorJoinLSHParams<ApproxVectorSimilarityJoinLSHBatchOp> {
    public ApproxVectorSimilarityJoinLSHBatchOp() {
        super(new Params());
    }

    public ApproxVectorSimilarityJoinLSHBatchOp(Params params) {
        super(params);
    }

    @Override
    public ApproxVectorSimilarityJoinLSHBatchOp linkFrom(BatchOperator<?>... inputs) {
        checkOpSize(2, inputs);

        String leftIdCol = this.getLeftIdCol();
        String rightIdCol = this.getRightIdCol();

        DataSet<BaseLSH> lsh = LocalitySensitiveHashApproxFunctions.buildLSH(inputs[0], inputs[1], this.getParams());

        DataSet<Row> res = LocalitySensitiveHashApproxFunctions.approxSimilarityJoin(
            inputs[0].select(new String[] {leftIdCol, getLeftCol()}).getDataSet(),
            inputs[1].select(new String[] {rightIdCol, getRightCol()}).getDataSet(), this.getDistanceThreshold(), lsh);

        this.setOutput(res, getJoinOutputSchema(inputs, leftIdCol, rightIdCol));
        return this;
    }

    static TableSchema getJoinOutputSchema(BatchOperator[] inputs, String leftIdCol, String rightIdCol) {
        TypeInformation[] types = new TypeInformation[] {
            TableUtil.findColTypeWithAssertAndHint(inputs[0].getSchema(), leftIdCol),
            TableUtil.findColTypeWithAssertAndHint(inputs[1].getSchema(), rightIdCol),
            Types.DOUBLE};

        if (leftIdCol.equalsIgnoreCase(rightIdCol)) {
            leftIdCol = leftIdCol + "_left";
            rightIdCol = rightIdCol + "_right";
        }

        String[] names = new String[] {leftIdCol, rightIdCol, DISTANCE_COL};

        return new TableSchema(names, types);
    }
}
