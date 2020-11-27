package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.common.HasPrimaryKeys;
import com.alibaba.alink.params.io.shared.HasCatalogName;
import com.alibaba.alink.params.io.shared.HasDefaultDatabase;
import com.alibaba.alink.params.io.shared.HasDriverName;
import com.alibaba.alink.params.io.shared.HasPasswordDefaultAsNull;
import com.alibaba.alink.params.io.shared.HasPluginVersion;
import com.alibaba.alink.params.io.shared.HasUrl;
import com.alibaba.alink.params.io.shared.HasUsernameDefaultAsNull;

public interface JdbcCatalogParams<T> extends
	HasCatalogName <T>,
	HasDefaultDatabase <T>,
	HasPrimaryKeys <T>,
	HasUsernameDefaultAsNull <T>,
	HasPasswordDefaultAsNull <T>,
	HasUrl <T>,
	HasDriverName <T>,
	HasPluginVersion <T> {
}
