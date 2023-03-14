package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.catalog.BaseCatalog;
import com.alibaba.alink.params.io.HasCatalogObject;

/**
 * catalog source batch op for Hive, Derby, Mysql, Sqlite.
 */
@IoOpAnnotation(name = "catalog", ioType = IOType.SourceBatch)
@NameCn("Catalog读入")
@NameEn("Catalog Source")
public class CatalogSourceBatchOp extends BaseSourceBatchOp <CatalogSourceBatchOp>
	implements HasCatalogObject <CatalogSourceBatchOp> {

	private static final long serialVersionUID = -8418005104042483591L;

	public CatalogSourceBatchOp() {
		this(null);
	}

	public CatalogSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(CatalogSourceBatchOp.class), params);
	}

	@Override
	protected Table initializeDataSource() {

		CatalogObject catalogObject = getCatalogObject();

		try (BaseCatalog catalog = catalogObject.getCatalog()) {

			catalog.open();

			return catalogObject.getCatalog().sourceBatch(
				catalogObject.getObjectPath(), catalogObject.getParams(), getMLEnvironmentId()
			);
		}
	}
}
