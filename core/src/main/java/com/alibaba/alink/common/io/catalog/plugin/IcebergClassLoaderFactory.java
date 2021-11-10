package com.alibaba.alink.common.io.catalog.plugin;

import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDescriptor;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.function.Function;
import java.util.function.Predicate;

public class IcebergClassLoaderFactory extends ClassLoaderFactory implements Serializable {
  private static final long serialVersionUID = 1233515335175475912L;

  private final static Logger LOG = LoggerFactory.getLogger(IcebergClassLoaderFactory.class);
  private static final String ICEBERG_DB_NAME = "iceberg";

  public IcebergClassLoaderFactory(String version) {
    super(new RegisterKey(ICEBERG_DB_NAME, version), ClassLoaderContainer.createPluginContextOnClient());
  }

  @Override
  public <T> T doAs(PrivilegedExceptionAction<T> action) throws Exception {

    ClassLoader classLoader = create();

    try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
      return action.run();
    }
  }

  @Override
  public ClassLoader create() {
    return ClassLoaderContainer
        .getInstance()
        .create(
            registerKey,
            registerContext,
            Factory.class,
            new IcebergServiceFilter(),
            new IcebergCatalogVersionGetter()
        );
  }

  private static class IcebergServiceFilter implements Predicate<Factory> {

    @Override
    public boolean test(Factory factory) {
      return factory.getClass().getName().contains("FlinkCatalogFactory");
    }
  }

  private static class IcebergCatalogVersionGetter implements
      Function<Tuple2<Factory, PluginDescriptor>, String> {

    @Override
    public String apply(Tuple2<Factory, PluginDescriptor> factory) {
      return factory.f1.getVersion();
    }
  }
}