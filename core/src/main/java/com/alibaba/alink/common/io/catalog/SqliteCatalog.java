package com.alibaba.alink.common.io.catalog;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
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
import com.alibaba.alink.common.io.plugin.wrapper.RichInputFormatWithClassLoader;
import com.alibaba.alink.common.io.plugin.wrapper.RichOutputFormatWithClassLoader;
import com.alibaba.alink.params.io.SqliteCatalogParams;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CatalogAnnotation(name = "sqlite")
public class SqliteCatalog extends JdbcCatalog {

	public static final String CATALOG_TYPE_VALUE_SQLITE = "sqlite";
	public static final String CATALOG_SQLITE_URLS = "dbUrls";
	public static final String CATALOG_SQLITE_USERNAME = "userName";
	public static final String CATALOG_SQLITE_PASSWORD = "password";

	private final JdbcCatalogClassLoaderFactory classLoaderFactory;

	private JdbcCatalog internal;

	public SqliteCatalog(String catalogName, String defaultDatabase, String sqliteVersion, String dbUrl) {
		this(catalogName, defaultDatabase, sqliteVersion, new String[] {dbUrl});
	}

	public SqliteCatalog(String catalogName, String defaultDatabase, String sqliteVersion, String[] dbUrls) {
		this(catalogName, defaultDatabase, sqliteVersion, dbUrls, null, null);
	}

	public SqliteCatalog(
		String catalogName, String defaultDatabase, String sqliteVersion,
		String[] dbUrls, String userName, String password) {
		this(
			new Params()
				.set(SqliteCatalogParams.CATALOG_NAME, catalogName)
				.set(SqliteCatalogParams.DEFAULT_DATABASE, defaultDatabase == null ? parseDbNameFromUrl(dbUrls[0]) : defaultDatabase)
				.set(SqliteCatalogParams.URLS, dbUrls)
				.set(SqliteCatalogParams.USERNAME, userName)
				.set(SqliteCatalogParams.PASSWORD, password)
				.set(SqliteCatalogParams.PLUGIN_VERSION, sqliteVersion)
		);
	}

	public SqliteCatalog(Params params) {
		super(params);

		classLoaderFactory = new JdbcCatalogClassLoaderFactory(
			CATALOG_TYPE_VALUE_SQLITE, getParams().get(SqliteCatalogParams.PLUGIN_VERSION)
		);
	}

	private static String parseDbNameFromUrl(String url) {
		String dbFilename = new Path(url).getName();
		int dotPos = dbFilename.lastIndexOf(".");
		return dbFilename.substring(0, dotPos < 0 ? dbFilename.length() : dotPos);
	}

	@Override
	public void open() throws CatalogException {
		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().open());
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
	protected ObjectPath rewriteObjectPath(ObjectPath objectPath) {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().rewriteObjectPath(objectPath));
	}

	@Override
	protected String rewriteDbUrl(String url, ObjectPath objectPath) {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().rewriteDbUrl(url, objectPath));
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
				.loadClass("com.alibaba.alink.common.io.catalog.sqlite.factories.SqliteCatalogFactory")
				.getConstructor()
				.newInstance();
		} catch (ClassNotFoundException | NoSuchMethodException
			| InstantiationException | IllegalAccessException | InvocationTargetException e) {

			throw new RuntimeException("Could not find the sqlite catelog factory.", e);
		}
	}

	private static JdbcCatalog createCatalog(Params params, ClassLoader classLoader) {
		String catalogName = params.get(SqliteCatalogParams.CATALOG_NAME);

		CatalogFactory factory = createCatalogFactory(classLoader);

		List <String> supportedKeys = factory.supportedProperties();

		if (!supportedKeys.contains(CATALOG_SQLITE_URLS)
			|| !supportedKeys.contains(CATALOG_SQLITE_USERNAME)
			|| !supportedKeys.contains(CATALOG_SQLITE_PASSWORD)) {

			throw new IllegalStateException(
				"Incorrect sqlite dependency. Please check the configure of sqlite environment."
			);
		}

		Map <String, String> properties = new HashMap <>();

		properties.put(CATALOG_SQLITE_URLS, Joiner.on(",").join(params.get(SqliteCatalogParams.URLS)));

		if (params.get(SqliteCatalogParams.USERNAME) != null
			&& params.get(SqliteCatalogParams.PASSWORD) != null) {

			properties.put(CATALOG_SQLITE_USERNAME, params.get(SqliteCatalogParams.USERNAME));
			properties.put(CATALOG_SQLITE_PASSWORD, params.get(SqliteCatalogParams.PASSWORD));
		}

		if (params.get(SqliteCatalogParams.DEFAULT_DATABASE) != null) {
			properties.put(CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE,
				params.get(SqliteCatalogParams.DEFAULT_DATABASE));
		}

		properties.putAll(factory.requiredContext());

		return (JdbcCatalog) factory.createCatalog(catalogName, properties);
	}
}
