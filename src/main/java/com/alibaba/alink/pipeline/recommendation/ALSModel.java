package com.alibaba.alink.pipeline.recommendation;

import com.alibaba.alink.operator.common.recommendation.AlsModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.AlsPredictBatchOp;
import com.alibaba.alink.params.recommendation.AlsPredictParams;
import com.alibaba.alink.pipeline.MapModel;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

/**
 * Model fitted by ALS.
 */
public class ALSModel extends MapModel<ALSModel> implements AlsPredictParams<ALSModel> {

    public ALSModel(Params params) {
        super(AlsModelMapper::new, params);
    }

    /**
     * Recommend for input users "numItems" items.
     */
//    public Table recommendForUserSubset(Table in, int numItems) {
//        AlsPredictBatchOp predictor = new AlsPredictBatchOp(this.getParams())
//            .setTopK(numItems);
//        return predictor.recommendForUsers(
//            BatchOperator.fromTable(this.getModelData()),
//            BatchOperator.fromTable(in)
//        );
//    }

    /**
     * Recommend for input items "numUsers" users.
     */
//    public Table recommendForItemSubset(Table in, int numUsers) {
//        AlsPredictBatchOp predictor = new AlsPredictBatchOp(this.getParams())
//            .setTopK(numUsers);
//        return predictor.recommendForItems(
//            BatchOperator.fromTable(this.getModelData()),
//            BatchOperator.fromTable(in)
//        );
//    }
}