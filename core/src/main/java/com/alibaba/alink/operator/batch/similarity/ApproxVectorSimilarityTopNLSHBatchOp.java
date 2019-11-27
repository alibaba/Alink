package com.alibaba.alink.operator.batch.similarity;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.BaseLSH;
import com.alibaba.alink.operator.common.feature.LocalitySensitiveHashApproxFunctions;
import com.alibaba.alink.params.similarity.ApproxVectorTopNLSHParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * ApproxVectorSimilarityTopNLSHBatchOp used to search the topN nearest neighbor of every record in
 * the first dataset from the second dataset. It's an approximate method using LSH.
 * <p>
 * The two datasets must each contain at least two columns: vector column and id column.
 * <p>
 * The class supports two distance type: EUCLIDEAND and JACCARD.
 * <p>
 * The output contains four columns: leftId, rightId, distance, rank.
 *
 */
public class ApproxVectorSimilarityTopNLSHBatchOp extends BatchOperator<ApproxVectorSimilarityTopNLSHBatchOp>
	implements ApproxVectorTopNLSHParams<ApproxVectorSimilarityTopNLSHBatchOp> {
	public ApproxVectorSimilarityTopNLSHBatchOp() {
		super(new Params());
	}

	public ApproxVectorSimilarityTopNLSHBatchOp(Params params) {
		super(params);
	}

	@Override
	public ApproxVectorSimilarityTopNLSHBatchOp linkFrom(BatchOperator<?>... inputs) {
		checkOpSize(2, inputs);
        checkOpSize(2, inputs);

        String leftIdCol = this.getLeftIdCol();
        String rightIdCol = this.getRightIdCol();

        DataSet<BaseLSH> lsh = LocalitySensitiveHashApproxFunctions.buildLSH(inputs[0], inputs[1], this.getParams());

        DataSet<Row> res = LocalitySensitiveHashApproxFunctions.approxNearestNeighbors(
            inputs[1].select(new String[] {rightIdCol, getRightCol()}).getDataSet(),
            inputs[0].select(new String[] {leftIdCol, getLeftCol()}).getDataSet(), this.getTopN(), lsh);

        if (leftIdCol.equals(rightIdCol)) {
            leftIdCol += "_left";
            rightIdCol += "_right";
        }

        this.setOutputTable(DataSetConversionUtil.toTable(getMLEnvironmentId(), res,
            new String[] {rightIdCol, leftIdCol, this.getOutputCol(), "rank"},
            new TypeInformation[] {TableUtil.findColType(inputs[1].getSchema(), this.getRightIdCol()),
                TableUtil.findColType(inputs[0].getSchema(), this.getLeftIdCol()),
                Types.DOUBLE,
                Types.LONG}
        ));
        return this;
	}
}
