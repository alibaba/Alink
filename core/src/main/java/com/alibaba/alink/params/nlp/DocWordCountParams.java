package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.shared.delimiter.HasWordDelimiter;

public interface DocWordCountParams<T> extends
	HasDocIdCol <T>,
	HasContentCol <T>,
	HasWordDelimiter <T> {
}
