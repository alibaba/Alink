package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.io.HasSchemaStr;
import com.alibaba.alink.params.shared.HasHandleInvalid;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

public interface StringToColumnsParams<T> extends
    HasSelectedCol<T>,
    HasSchemaStr<T>,
    HasReservedCols<T>,
    HasHandleInvalid<T> {}