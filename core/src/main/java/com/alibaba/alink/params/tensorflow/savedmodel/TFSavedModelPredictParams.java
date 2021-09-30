
package com.alibaba.alink.params.tensorflow.savedmodel;

import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.tensorflow.savedmodel.BaseTFSavedModelPredictParams;

public interface TFSavedModelPredictParams<T> extends BaseTFSavedModelPredictParams <T>, HasModelPath <T> {
}
