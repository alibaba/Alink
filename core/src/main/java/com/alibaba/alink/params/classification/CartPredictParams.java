package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.HasNumThreads;
public interface CartPredictParams<T> extends RichModelMapperParams <T>, HasNumThreads <T> {
}
