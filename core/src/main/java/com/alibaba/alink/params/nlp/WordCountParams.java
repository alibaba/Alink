package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.delimiter.HasWordDelimiter;

public interface WordCountParams<T> extends
	HasSelectedCol <T>,
	HasWordDelimiter <T> {
}
