package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.delimiter.HasColDelimiterDvComma;
import com.alibaba.alink.params.shared.delimiter.HasValDelimiterDvColon;

public interface KvToColumnsParams<T> extends
    StringToColumnsParams<T>, HasColDelimiterDvComma<T>, HasValDelimiterDvColon<T> {
}