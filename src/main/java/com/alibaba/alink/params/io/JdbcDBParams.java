package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.common.HasPrimaryKeys;
import com.alibaba.alink.params.io.shared_params.HasDriverName;
import com.alibaba.alink.params.io.shared_params.HasPassword_null;
import com.alibaba.alink.params.io.shared_params.HasUrl;
import com.alibaba.alink.params.io.shared_params.HasUsername_null;

public interface JdbcDBParams<T> extends
	HasPrimaryKeys <T>, HasUsername_null <T>, HasPassword_null <T>, HasUrl <T>, HasDriverName <T> {
}
