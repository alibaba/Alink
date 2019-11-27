package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.shared.HasVectorSizeDv100;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.delimiter.HasWordDelimiter;
import com.alibaba.alink.params.shared.iter.HasNumIterDefaultAs1;

public interface Word2VecTrainParams<T> extends
    HasNumIterDefaultAs1<T>,
	HasSelectedCol <T>,
	HasVectorSizeDv100 <T>,
	HasAlpha <T>,
	HasWordDelimiter <T>,
	HasMinCount <T>,
	HasRandomWindow <T>,
	HasWindow <T> {
}
