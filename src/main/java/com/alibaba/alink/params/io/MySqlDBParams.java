package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.shared_params.*;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface MySqlDBParams<T> extends WithParams<T>,
    HasDbName<T>, HasIp<T>, HasPassword<T>, HasPort<T>, HasUsername<T> {
}
