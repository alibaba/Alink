package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;

/**
 * parameters of linear classifier.
 */
public interface ClassifierTrainParams<T> extends
	HasFeatureColsDefaultAsNull <T>,
	HasLabelCol <T> {}
