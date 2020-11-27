package com.alibaba.alink.operator.stream.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.catalog.BaseCatalog;
import com.alibaba.alink.params.io.HasCatalogObject;

/**
 * catalog source stream op for Hive, Derby, Mysql, Sqlite.
 */
@IoOpAnnotation(name = "catalog", ioType = IOType.SourceStream)
public class CatalogSourceStreamOp extends BaseSourceStreamOp <CatalogSourceStreamOp>
	implements HasCatalogObject <CatalogSourceStreamOp> {

	private static final long serialVersionUID = -1252642410016754979L;

	public CatalogSourceStreamOp() {
		this(null);
	}

	public CatalogSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(CatalogSourceStreamOp.class), params);
	}

	@Override
	protected Table initializeDataSource() {

		CatalogObject catalogObject = getCatalogObject();

		try (BaseCatalog catalog = catalogObject.getCatalog()) {

			catalog.open();

			return catalog.sourceStream(
				catalogObject.getObjectPath(), catalogObject.getParams(), getMLEnvironmentId()
			);
		}
	}
}
