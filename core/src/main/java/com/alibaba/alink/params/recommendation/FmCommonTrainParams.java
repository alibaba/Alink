package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.recommendation.fm.HasInitStdevDefaultAs005;
import com.alibaba.alink.params.recommendation.fm.HasLambda0DefaultAs0;
import com.alibaba.alink.params.recommendation.fm.HasLambda1DefaultAs0;
import com.alibaba.alink.params.recommendation.fm.HasLambda2DefaultAs0;
import com.alibaba.alink.params.recommendation.fm.HasLearnRateDefaultAs001;
import com.alibaba.alink.params.recommendation.fm.HasLinearItemDefaultAsTrue;
import com.alibaba.alink.params.recommendation.fm.HasNumEpochDefaultAs10;
import com.alibaba.alink.params.recommendation.fm.HasNumFactorsDefaultAs10;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;

public interface FmCommonTrainParams<T> extends
        HasWithIntercept<T>,
        HasLinearItemDefaultAsTrue<T>,
        HasNumFactorsDefaultAs10<T>,
        HasLambda0DefaultAs0<T>,
        HasLambda1DefaultAs0<T>,
        HasLambda2DefaultAs0<T>,
        HasNumEpochDefaultAs10<T>,
        HasLearnRateDefaultAs001<T>,
        HasInitStdevDefaultAs005<T> {
}
