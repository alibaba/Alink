package com.alibaba.alink.operator.common.prophet;

import com.alibaba.alink.params.dataproc.HasWithMean;
import com.alibaba.alink.params.dataproc.HasWithStd;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * prophet needed params
 * @param <T>
 */
public interface ProphetTrainParams<T> extends HasSelectedCols<T>,
        HasWithMean<T>,
        HasWithStd<T> {}
