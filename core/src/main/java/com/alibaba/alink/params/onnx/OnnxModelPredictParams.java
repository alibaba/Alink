package com.alibaba.alink.params.onnx;

import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.tensorflow.savedmodel.HasGraphDefTag;
import com.alibaba.alink.params.tensorflow.savedmodel.HasInputSignatureDefs;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSignatureDefs;
import com.alibaba.alink.params.tensorflow.savedmodel.HasSignatureDefKey;

public interface OnnxModelPredictParams<T> extends HasModelPath <T>,
	HasReservedColsDefaultAsNull <T>,
	HasInputNames <T>, HasOutputNames <T>,
	HasSelectedColsDefaultAsNull <T>, HasOutputSchemaStr <T> {
}
