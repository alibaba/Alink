package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.io.shared.HasCatalogName;
import com.alibaba.alink.params.io.shared.HasDefaultDatabase;
import com.alibaba.alink.params.io.shared.HasPartition;
import com.alibaba.alink.params.io.shared.HasPartitions;
import com.alibaba.alink.params.io.shared.HasPluginVersion;
import com.alibaba.alink.params.shared.HasOverwriteSink;

public interface HiveCatalogParams<T>
	extends HasCatalogName <T>,
	HasDefaultDatabase <T>,
	HasPluginVersion <T>,
	HasPartitions <T>,
	HasPartition <T>,
	HasOverwriteSink <T> {

	ParamInfo <String> HIVE_CONF_DIR = ParamInfoFactory
		.createParamInfo("hiveConfDir", String.class)
		.setDescription("Hive configuration directory")
		.setRequired()
		.build();

	default T setHiveConfDir(String value) {
		return set(HIVE_CONF_DIR, value);
	}

	default String getHiveConfDir() {
		return get(HIVE_CONF_DIR);
	}

	ParamInfo <String> KERBEROS_PRINCIPAL = ParamInfoFactory
		.createParamInfo("kerberosPrincipal", String.class)
		.setDescription("kerberosPrincipal")
		.setHasDefaultValue(null)
		.build();

	default T setKerberosPrincipal(String value) {
		return set(KERBEROS_PRINCIPAL, value);
	}

	default String getKerberosPrincipal() {
		return get(KERBEROS_PRINCIPAL);
	}

	ParamInfo <String> KERBEROS_KEYTAB = ParamInfoFactory
		.createParamInfo("kerberosKeytab", String.class)
		.setDescription("kerberosKeytab")
		.setHasDefaultValue(null)
		.build();

	default T setKerberosKeytab(String value) {
		return set(KERBEROS_KEYTAB, value);
	}

	default String getKerberosKeytab() {
		return get(KERBEROS_KEYTAB);
	}
}
