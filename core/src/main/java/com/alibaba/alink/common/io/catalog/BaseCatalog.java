package com.alibaba.alink.common.io.catalog;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.ObjectPath;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.CatalogAnnotationUtils;
import com.alibaba.alink.params.io.HasIoName;
import com.alibaba.alink.params.io.shared.HasCatalogName;
import com.alibaba.alink.params.io.shared.HasDefaultDatabase;

import java.util.UUID;

/**
 * Only for flink community.
 */
public abstract class BaseCatalog extends AbstractCatalog implements AutoCloseable {

	private final Params params;

	public BaseCatalog(Params params) {
		super(
			params == null ? null : params.get(HasCatalogName.CATALOG_NAME),
			params == null ? null : params.get(HasDefaultDatabase.DEFAULT_DATABASE)
		);

		if (null == params) {
			this.params = new Params();
		} else {
			this.params = params.clone();
		}

		this.params.set(HasIoName.IO_NAME, CatalogAnnotationUtils.annotatedName(this.getClass()));
	}

	public static BaseCatalog of(Params params) throws Exception {
		if (isCatalog(params)) {
			return CatalogAnnotationUtils.createCatalog(params.get(HasIoName.IO_NAME), params);
		} else {
			throw new IllegalArgumentException("NOT a catalog parameter.");
		}
	}

	static boolean isCatalog(Params params) {
		if (params.contains(HasIoName.IO_NAME)) {
			return CatalogAnnotationUtils.isCatalog(params.get(HasIoName.IO_NAME));
		} else {
			return false;
		}
	}

	public Params getParams() {
		return params;
	}

	public static String genRandomCatalogName() {
		return "catalog_" + UUID.randomUUID().toString().replaceAll("-", "_");
	}

	/**
	 * Get Flink Table object of given table.
	 *
	 * @param objectPath the table path
	 * @param params     params.
	 * @param sessionId  ML Environment session id.
	 * @return flink table.
	 */
	public abstract Table sourceStream(ObjectPath objectPath, Params params, Long sessionId);

	/**
	 * Sink data in flink table into target table in database with default ML Environment.
	 *  @param objectPath the table path
	 * @param in         flink table to sink.
	 * @param params     params.
	 * @param sessionId ML Environment session id.
	 */
	public abstract void sinkStream(ObjectPath objectPath, Table in, Params params, Long sessionId);

	/**
	 * Get batch source of given table.
	 *
	 * @param objectPath the table path
	 * @param params     parameter.
	 * @param sessionId  ML Environment session id.
	 * @return flink table.
	 */
	public abstract Table sourceBatch(ObjectPath objectPath, Params params, Long sessionId);

	/**
	 * Sink a data in flink table into target table in database.
	 *  @param objectPath the table path.
	 * @param in         flink table to sink.
	 * @param params     params.
	 * @param sessionId ML Environment session id.
	 */
	public abstract void sinkBatch(ObjectPath objectPath, Table in, Params params, Long sessionId);

}
