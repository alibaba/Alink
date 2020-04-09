package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Parameters for {@link com.alibaba.alink.pipeline.dataproc.StringIndexerModel}.
 */
public interface StringIndexerPredictParams<T> extends
    HasSelectedCol<T>,
    HasReservedCols<T>,
    HasHandleInvalid<T>,
    HasOutputColDefaultAsNull<T> {
}
