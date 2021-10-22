package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.shared.HasCatalogName;
import com.alibaba.alink.params.io.shared.HasDefaultDatabase;
import com.alibaba.alink.params.io.shared.HasPartition;
import com.alibaba.alink.params.io.shared.HasPartitions;
import com.alibaba.alink.params.io.shared.HasPluginVersion;
import com.alibaba.alink.params.shared.HasOverwriteSink;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface IcebergCatalogParams<T>
    extends HasCatalogName<T>,
    HasDefaultDatabase<T>,
    HasPluginVersion<T>,
    HasOverwriteSink<T> {


  ParamInfo<String> CATALOG_TYPE = ParamInfoFactory
      .createParamInfo("catalog-type", String.class)
      .setDescription("Iceberg currently support hive or hadoop catalog type")
      .setRequired()
      .build();

  default T setCatalogType(String value) {
    return set(CATALOG_TYPE, value);
  }

  default String getCatalogType() {
    return get(CATALOG_TYPE);
  }

  ParamInfo<String> WAREHOUSE = ParamInfoFactory
      .createParamInfo("warehouse", String.class)
      .setDescription(
          "The Hive warehouse location.The Hive warehouse location, users should specify this path" +
              " if neither set the hive-conf-dir to specify a location containing a hive-site.xml " +
              "configuration file nor add a correct hive-site.xml to classpath")
      .build();

  default T setWarehouse(String value) {
    return set(WAREHOUSE, value);
  }

  default String getWarehouse() {
    return get(WAREHOUSE);
  }


  ParamInfo<String> HIVE_CONF_DIR = ParamInfoFactory
      .createParamInfo("hiveConfDir", String.class)
      .setDescription("Hive configuration directory")
      .build();

  default T setHiveConfDir(String value) {
    return set(HIVE_CONF_DIR, value);
  }

  default String getHiveConfDir() {
    return get(HIVE_CONF_DIR);
  }

  ParamInfo<String> HIVE_URI = ParamInfoFactory
      .createParamInfo("uri", String.class)
      .setDescription("Hive metastore uri ")
      .build();

  default T setHiveUri(String value) {
    return set(HIVE_URI, value);
  }

  default String getHiveUri() {
    return get(HIVE_URI);
  }
}
