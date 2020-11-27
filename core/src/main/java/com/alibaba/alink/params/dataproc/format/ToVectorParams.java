package com.alibaba.alink.params.dataproc.format;

import com.alibaba.alink.params.shared.colname.HasVectorCol;

public interface ToVectorParams<T> extends
	HasVectorCol <T>,
	HasVectorSize <T> {
}