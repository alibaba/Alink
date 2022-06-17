package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasTimeCol;

public interface GenerateFeatureOfWindowParams<T> extends HasTimeCol <T>, HasWindowFeatureDefinitions <T> {
}
