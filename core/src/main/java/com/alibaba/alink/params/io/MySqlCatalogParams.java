package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.shared.HasIp;
import com.alibaba.alink.params.io.shared.HasPort;

public interface MySqlCatalogParams<T>
	extends JdbcCatalogParams<T>, HasIp <T>, HasPort <T>  {
}
