package com.alibaba.alink.params.tensorflow;

import com.alibaba.alink.params.dl.BaseDLTableModelTrainParams;
import com.alibaba.alink.params.dl.HasNumPssDefaultAsNull;

public interface TFTableModelTrainParams<T> extends BaseDLTableModelTrainParams <T>, HasNumPssDefaultAsNull <T> {
}
