package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.nlp.walk.HasTypeCol;
import com.alibaba.alink.params.nlp.walk.HasVertexCol;

public interface LabeledWord2VecParams<T> extends
	Word2VecParams <T>,
	HasVertexCol <T>,
	HasTypeCol <T> {
}
