package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.catalog.BaseCatalog;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.HasCatalogObject;

/**
 * catalog sink batch op for Hive, Derby, Mysql, Sqlite.
 */
@IoOpAnnotation(name = "catalog", ioType = IOType.SinkBatch)
@NameCn("Catalog数据表导出")
public class CatalogSinkBatchOp extends BaseSinkBatchOp <CatalogSinkBatchOp>
	implements HasCatalogObject <CatalogSinkBatchOp> {

	private static final long serialVersionUID = 9150670049270862052L;

	public CatalogSinkBatchOp() {
		this(null);
	}

	public CatalogSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(CatalogSinkBatchOp.class), params);
	}

	@Override
	protected CatalogSinkBatchOp sinkFrom(BatchOperator <?> in) {

		CatalogObject catalogObject = getCatalogObject();

		try (BaseCatalog catalog = catalogObject.getCatalog()){

			catalog.open();

			catalog.sinkBatch(
				catalogObject.getObjectPath(),
				in.getOutputTable(),
				catalogObject.getParams(),
				in.getMLEnvironmentId()
			);

		}

		return this;
	}
}
