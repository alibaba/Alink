package com.alibaba.alink.params.io;

import com.alibaba.alink.params.shared.HasModelFilePath;
import com.alibaba.alink.params.shared.HasOverwriteSink;

public interface ModelFileSinkParams<T> extends HasModelFilePath <T>, HasOverwriteSink <T> {
}
