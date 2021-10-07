package com.alibaba.alink.common.io.catalog;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.annotations.CatalogAnnotation;
import com.alibaba.alink.common.io.catalog.plugin.JdbcCatalogClassLoaderFactory;
import com.alibaba.alink.common.io.catalog.plugin.RichInputFormatWithClassLoader;
import com.alibaba.alink.common.io.catalog.plugin.RichOutputFormatWithClassLoader;
import com.alibaba.alink.params.io.MySqlCatalogParams;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CatalogAnnotation(name = "mysql")
public class MySqlCatalog extends JdbcCatalog {
	public static final String CATALOG_TYPE_VALUE_MYSQL = "mysql";
	public static final String CATALOG_MYSQL_URL = "mysqlUrl";
	public static final String CATALOG_MYSQL_PORT = "port";
	public static final String CATALOG_MYSQL_USERNAME = "userName";
	public static final String CATALOG_MYSQL_PASSWORD = "password";

	private final JdbcCatalogClassLoaderFactory classLoaderFactory;
	private JdbcCatalog internal;

	public MySqlCatalog(
		String catalogName, String defaultDatabase, String mysqlVersion,
		String mysqlUrl, String port) {

		this(catalogName, defaultDatabase, mysqlVersion, mysqlUrl, port, null, null);
	}

	public MySqlCatalog(
		String catalogName, String defaultDatabase, String mysqlVersion,
		String mysqlUrl, String port, String userName, String password) {

		this(
			new Params()
				.set(MySqlCatalogParams.CATALOG_NAME, catalogName == null ? genRandomCatalogName() : catalogName)
				.set(MySqlCatalogParams.DEFAULT_DATABASE, defaultDatabase == null ? "default" : defaultDatabase)
				.set(MySqlCatalogParams.URL, mysqlUrl)
				.set(MySqlCatalogParams.PORT, port)
				.set(MySqlCatalogParams.USERNAME, userName)
				.set(MySqlCatalogParams.PASSWORD, password)
				.set(MySqlCatalogParams.PLUGIN_VERSION, mysqlVersion)
		);
	}

	public MySqlCatalog(Params params) {
		super(params);

		classLoaderFactory = new JdbcCatalogClassLoaderFactory(
			CATALOG_TYPE_VALUE_MYSQL, getParams().get(MySqlCatalogParams.PLUGIN_VERSION)
		);
	}

	@Override
	public void open() throws CatalogException {
		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().open());
	}

	@Override
	public void close() throws CatalogException {
		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().close());
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName)
		throws DatabaseNotExistException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getDatabase(databaseName));
	}

	@Override
	public List <String> listDatabases() throws CatalogException {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listDatabases());
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().createDatabase(name, database, ignoreIfExists));
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {

		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().databaseExists(databaseName));
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().dropDatabase(name, ignoreIfNotExists));
	}

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().alterDatabase(name, newDatabase, ignoreIfNotExists));
	}

	@Override
	public List <String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listTables(databaseName));
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().tableExists(tablePath));
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().createTable(tablePath, table, ignoreIfExists));
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getTable(tablePath));
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {
		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().dropTable(tablePath, ignoreIfNotExists));
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().renameTable(tablePath, newTableName, ignoreIfNotExists));
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().alterTable(tablePath, newTable, ignoreIfNotExists));
	}

	@Override
	public List <String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listViews(databaseName));
	}

	@Override
	protected int flinkType2JdbcType(DataType flinkType) {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().flinkType2JdbcType(flinkType));
	}

	@Override
	protected RichInputFormat <Row, InputSplit> createInputFormat(ObjectPath objectPath, TableSchema schema)
		throws Exception {
		return new RichInputFormatWithClassLoader <>(classLoaderFactory,
			loadCatalog().createInputFormat(objectPath, schema));
	}

	@Override
	protected OutputFormat <Row> createOutputFormat(ObjectPath objectPath, TableSchema schema, String sql) {
		return new RichOutputFormatWithClassLoader(classLoaderFactory,
			loadCatalog().createOutputFormat(objectPath, schema, sql));
	}

	private JdbcCatalog loadCatalog() {
		if (internal == null) {
			internal = classLoaderFactory.doAsThrowRuntime(
				() -> createCatalog(getParams(), Thread.currentThread().getContextClassLoader())
			);
		}

		return internal;
	}

	public static CatalogFactory createCatalogFactory(ClassLoader classLoader) {
		try {
			return (CatalogFactory) classLoader
				.loadClass("com.alibaba.alink.common.io.catalog.mysql.factories.MySqlCatalogFactory")
				.getConstructor()
				.newInstance();
		} catch (ClassNotFoundException | NoSuchMethodException
			| InstantiationException | IllegalAccessException | InvocationTargetException e) {

			throw new RuntimeException("Could not find the mysql catelog factory.", e);
		}
	}

	private static JdbcCatalog createCatalog(Params params, ClassLoader classLoader) {
		String catalogName = params.get(MySqlCatalogParams.CATALOG_NAME);

		CatalogFactory factory = createCatalogFactory(classLoader);

		List <String> supportedKeys = factory.supportedProperties();

		if (!supportedKeys.contains(CATALOG_MYSQL_URL)
			|| !supportedKeys.contains(CATALOG_MYSQL_PORT)
			|| !supportedKeys.contains(CATALOG_MYSQL_USERNAME)
			|| !supportedKeys.contains(CATALOG_MYSQL_PASSWORD)) {

			throw new IllegalStateException(
				"Incorrect mysql dependency. Please check the configure of mysql environment."
			);
		}

		Map <String, String> properties = new HashMap <>();

		properties.put(CATALOG_MYSQL_URL, params.get(MySqlCatalogParams.URL));
		properties.put(CATALOG_MYSQL_PORT, params.get(MySqlCatalogParams.PORT));

		if (params.get(MySqlCatalogParams.USERNAME) != null
			&& params.get(MySqlCatalogParams.PASSWORD) != null) {

			properties.put(CATALOG_MYSQL_USERNAME, params.get(MySqlCatalogParams.USERNAME));
			properties.put(CATALOG_MYSQL_PASSWORD, params.get(MySqlCatalogParams.PASSWORD));
		}

		if (params.get(MySqlCatalogParams.DEFAULT_DATABASE) != null) {
			properties.put(CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE,
				params.get(MySqlCatalogParams.DEFAULT_DATABASE));
		}

		properties.putAll(factory.requiredContext());

		return (JdbcCatalog) factory.createCatalog(catalogName, properties);
	}
}
