package com.alibaba.alink.common.io.catalog.plugin;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.factories.TableFactory;

import com.alibaba.alink.common.io.catalog.HiveBaseUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.PluginDescriptor;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.params.io.HiveCatalogParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class HiveClassLoaderFactory extends ClassLoaderFactory implements Serializable {
	private static final long serialVersionUID = 1233515535175478984L;

	private final static Logger LOG = LoggerFactory.getLogger(HiveClassLoaderFactory.class);
	private static final String HIVE_DB_NAME = "hive";

	private final Params actionContext;

	private transient MapFunction <PrivilegedExceptionAction <Object>, Object> internal;

	public HiveClassLoaderFactory(String version, Params actionContext) {
		super(
			new RegisterKey(HIVE_DB_NAME, version),
			PluginDistributeCache.createDistributeCache(HIVE_DB_NAME, version)
		);
		this.actionContext = actionContext;
	}

	@Override
	public <T> T doAs(PrivilegedExceptionAction <T> action) throws Exception {

		ClassLoader classLoader = create();

		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			if (actionContext.get(HiveCatalogParams.KERBEROS_PRINCIPAL) == null
				|| actionContext.get(HiveCatalogParams.KERBEROS_KEYTAB) == null) {

				return action.run();
			}

			if (internal == null) {
				setupKrb5Conf(FilePath.deserialize(actionContext.get(HiveCatalogParams.HIVE_CONF_DIR)));

				String kerberosPrincipal = actionContext.get(HiveCatalogParams.KERBEROS_PRINCIPAL);
				FilePath filePath = FilePath.deserialize(actionContext.get(HiveCatalogParams.KERBEROS_KEYTAB));

				String kerberosKeytab;
				kerberosKeytab = new Path(
					HiveBaseUtils.downloadFolder(
						new FilePath(filePath.getPath().getParent(), filePath.getFileSystem()),
						filePath.getPath().getName()
					),
					filePath.getPath().getName()
				).toString();

				internal = createDoAs(kerberosPrincipal, kerberosKeytab, create());
			}

			return (T) internal.map((PrivilegedExceptionAction <Object>) action);
		}
	}

	private static final String KRB5_CONF_KEY = "java.security.krb5.conf";
	private static final String KRB5_CONF_FILE_NAME = "krb5.conf";

	private void setupKrb5Conf(FilePath hiveConfDir) {
		final String oldValue = System.getProperty(KRB5_CONF_KEY, "");
		if (oldValue != null && oldValue.length() > 0 && Files.exists(Paths.get(oldValue))) {
			// the krb5.conf already exists in System.Properties
			return;
		}
		try {
			if (!HiveBaseUtils.fileExists(hiveConfDir, KRB5_CONF_FILE_NAME)) {
				LOG.warn("failed to find {} in {}, ignore it", KRB5_CONF_FILE_NAME, hiveConfDir.getPath());
				return;
			}
		} catch (IOException e) {
			LOG.warn("failed to find {} in {}", KRB5_CONF_FILE_NAME, hiveConfDir.getPath(), e);
			return;
		}

		final String localConfDir;
		try {
			localConfDir = HiveBaseUtils.downloadFolder(hiveConfDir, KRB5_CONF_FILE_NAME);
		} catch (IOException e) {
			LOG.warn("failed to download {} in {}", KRB5_CONF_FILE_NAME, hiveConfDir, e);
			return;
		}
		final String localKrb5File = new File(localConfDir, KRB5_CONF_FILE_NAME).getAbsolutePath();
		System.setProperty(KRB5_CONF_KEY, localKrb5File);
		LOG.info("reset {} to {} with remoteDir:{}", KRB5_CONF_KEY, localKrb5File, hiveConfDir.getPath());
	}

	@Override
	public ClassLoader create() {
		ClassLoader classLoader = ClassLoaderContainer
			.getInstance()
			.create(
				registerKey, distributeCache, TableFactory.class,
				new HiveServiceFilter(), new HiveVersionGetter()
			);

		if (classLoader != null) {
			installSecurity(classLoader);
		}

		return classLoader;
	}

	private transient Map <ClassLoader, Boolean> installed;

	private void installSecurity(ClassLoader classLoader) {
		if (installed == null) {
			installed = new HashMap <>();
		}

		if (!installed.computeIfAbsent(classLoader, HiveClassLoaderFactory::loginClassLoader)) {
			throw new IllegalStateException("Could not install security.");
		}
	}

	private static class HiveServiceFilter implements Predicate <TableFactory> {

		@Override
		public boolean test(TableFactory factory) {
			String catalogType = factory.requiredContext().get(CatalogDescriptorValidator.CATALOG_TYPE);
			return catalogType != null
				&& catalogType.equalsIgnoreCase("hive")
				&& factory.getClass().getName().contains("HiveCatalogFactory");
		}
	}

	public static MapFunction <PrivilegedExceptionAction <Object>, Object> createDoAs(
		String kerberosPrincipal, String kerberosKeytab, ClassLoader classLoader) {

		if (kerberosPrincipal == null || kerberosKeytab == null) {
			return null;
		}

		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {

			Class <?> initializer = Class.forName(
				"com.alibaba.alink.common.io.plugin.initializer.LoginUgi",
				true, classLoader
			);

			return (MapFunction <PrivilegedExceptionAction <Object>, Object>) initializer
				.getConstructor(String.class, String.class)
				.newInstance(kerberosPrincipal, kerberosKeytab);

		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException(
				String.format("Could not find LoginUgi. Init kerberos error, Principal: %s", kerberosPrincipal), e);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(
				String.format("Could not find the LoginUgi constructor. Init kerberos error, Principal: %s",
					kerberosPrincipal), e);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new IllegalArgumentException(
				String.format("Invoke the LoginUgi constructor error. Init kerberos error, Principal: %s",
					kerberosPrincipal), e);
		} catch (InstantiationException e) {
			throw new IllegalArgumentException(
				String.format("Create LoginUgi error. Init kerberos error, Principal: %s", kerberosPrincipal), e);
		}
	}

	private static final String HIVE_HDFS_CONFIG_KEY = "ALINK_HIVE_HDFS_CONFIG";

	private static Boolean loginClassLoader(ClassLoader classLoader) {
		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {

			if (System.getProperties().containsKey("java.security.krb5.conf") &&
				Files.exists(Paths.get(System.getProperty("java.security.krb5.conf")))) {

				Configuration configuration = GlobalConfiguration.loadConfiguration();

				if (!(configuration.containsKey(ConfigConstants.PATH_HADOOP_CONFIG))) {
					if (System.getProperties().containsKey(HIVE_HDFS_CONFIG_KEY)) {
						configuration.setString(
							ConfigConstants.PATH_HADOOP_CONFIG,
							System.getProperty(HIVE_HDFS_CONFIG_KEY)
						);
					} else if (System.getenv().containsKey(HIVE_HDFS_CONFIG_KEY)) {
						configuration.setString(
							ConfigConstants.PATH_HADOOP_CONFIG,
							System.getenv(HIVE_HDFS_CONFIG_KEY)
						);
					} else {
						throw new IllegalStateException(
							"There should config the PATH_HADOOP_CONFIG in flink configure.");
					}
				}

				try {

					Class <?> initializer = Class.forName(
						"com.alibaba.alink.common.io.plugin.initializer.HivePluginInitializer",
						true, classLoader
					);

					Method method = initializer.getMethod("initialize", String.class);

					method.invoke(null, configuration.getString(ConfigConstants.PATH_HADOOP_CONFIG, null));
				} catch (ClassNotFoundException
					| NoSuchMethodException
					| IllegalAccessException
					| InvocationTargetException e) {

					throw new RuntimeException(e);
				}
			}
		}

		return true;
	}

	private static class HiveVersionGetter implements Function <Tuple2 <TableFactory, PluginDescriptor>, String> {
		@Override
		public String apply(Tuple2 <TableFactory, PluginDescriptor> factory) {
			try {
				if (factory.f1.getVersion() != null) {
					return factory.f1.getVersion();
				}

				String version = (String) factory.f0.getClass()
					.getClassLoader()
					.loadClass("org.apache.flink.table.catalog.hive.client.HiveShimLoader")
					.getMethod("getHiveVersion")
					.invoke(null);

				int indexOfSlash = version.indexOf("-");
				return indexOfSlash < 0 ? version : version.substring(0, indexOfSlash);
			} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {

				LOG.warn("Cound not find the hive shim in class loader. factor: " + factory);

				// pass
				return factory.f1.getVersion();
			}
		}
	}
}