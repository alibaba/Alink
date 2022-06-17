package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.SwingRecommKernel;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;

@NameCn("swing推荐")
@NameEn("Swing Recommendation")
public class SwingRecommBatchOp extends BaseRecommBatchOp<SwingRecommBatchOp>
    implements BaseSimilarItemsRecommParams<SwingRecommBatchOp> {
    public SwingRecommBatchOp() {
        this(new Params());
    }

    public SwingRecommBatchOp(Params params) {
        super(SwingRecommKernel::new, RecommType.SIMILAR_ITEMS, params);
    }
}
