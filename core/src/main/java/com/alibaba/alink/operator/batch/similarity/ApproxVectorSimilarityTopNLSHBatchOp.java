package com.alibaba.alink.operator.batch.similarity;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.BaseLSH;
import com.alibaba.alink.operator.common.feature.LocalitySensitiveHashApproxFunctions;
import com.alibaba.alink.params.similarity.ApproxVectorTopNLSHParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
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
    static String DISTANCE_COL = "distance";
    static String RANK_COL = "rank";

    public ApproxVectorSimilarityTopNLSHBatchOp() {
		super(new Params());
	}

	public ApproxVectorSimilarityTopNLSHBatchOp(Params params) {
		super(params);
	}

    public static TableSchema getTopNOutputSchema(BatchOperator[] inputs, String leftIdCol, String rightIdCol){
        TypeInformation[] types = new TypeInformation[] {
            TableUtil.findColTypeWithAssertAndHint(inputs[1].getSchema(), rightIdCol),
            TableUtil.findColTypeWithAssertAndHint(inputs[0].getSchema(), leftIdCol),
            Types.DOUBLE, Types.LONG};

        if(leftIdCol.equalsIgnoreCase(rightIdCol)){
            leftIdCol = leftIdCol + "_left";
            rightIdCol = rightIdCol + "_right";
        }

        String[] names = new String[] {rightIdCol, leftIdCol, DISTANCE_COL,
            RANK_COL};

        return new TableSchema(names, types);
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

        this.setOutput(res, getTopNOutputSchema(inputs, leftIdCol, rightIdCol));
        return this;
	}
}
