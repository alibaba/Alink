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
import org.apache.flink.types.Row;

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

        if (leftIdCol.equals(rightIdCol)) {
            leftIdCol += "_left";
            rightIdCol += "_right";
        }

        this.setOutputTable(DataSetConversionUtil.toTable(getMLEnvironmentId(), res,
            new String[] {leftIdCol, rightIdCol, this.getOutputCol()},
            new TypeInformation[] {TableUtil.findColType(inputs[0].getSchema(), this.getLeftIdCol()),
                TableUtil.findColType(inputs[1].getSchema(), this.getRightIdCol()),
                Types.DOUBLE}
        ));
        return this;
    }
}
