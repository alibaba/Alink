package com.alibaba.alink.params.classification;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface KnnTrainParams<T> extends WithParams <T>,
	HasLabelCol <T>,
	HasFeatureColsDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T>,
	HasReservedColsDefaultAsNull <T>,
	HasKMeansDistanceType <T> {
}
