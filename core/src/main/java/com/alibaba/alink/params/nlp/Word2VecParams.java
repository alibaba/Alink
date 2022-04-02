package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.shared.HasVectorSizeDefaultAs100;
import com.alibaba.alink.params.shared.colname.HasSelectedColDefaultAsContent;
import com.alibaba.alink.params.shared.delimiter.HasWordDelimiter;
import com.alibaba.alink.params.shared.iter.HasNumIterDefaultAs1;

public interface Word2VecParams<T> extends HasNumIterDefaultAs1 <T>,
	HasSelectedColDefaultAsContent <T>,
	HasVectorSizeDefaultAs100 <T>,
	HasAlpha <T>,
	HasWordDelimiter <T>,
	HasMinCount <T>,
	HasNegative <T>,
	HasRandomWindow <T>,
	HasWindow <T>,
	HasBatchSize <T> {
}
