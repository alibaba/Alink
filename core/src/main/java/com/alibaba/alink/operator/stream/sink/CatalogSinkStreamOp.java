package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.catalog.BaseCatalog;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.HasCatalogObject;

/**
 * catalog sink stream op for Hive, Derby, Mysql, Sqlite.
 */
@IoOpAnnotation(name = "catalog", ioType = IOType.SinkStream)
@NameCn("Catalog数据表导出")
public class CatalogSinkStreamOp extends BaseSinkStreamOp <CatalogSinkStreamOp>
	implements HasCatalogObject <CatalogSinkStreamOp> {

	private static final long serialVersionUID = -4881169712574852323L;

	public CatalogSinkStreamOp() {
		this(null);
	}

	public CatalogSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(CatalogSinkStreamOp.class), params);
	}

	@Override
	protected CatalogSinkStreamOp sinkFrom(StreamOperator <?> in) {

		CatalogObject catalogObject = getCatalogObject();

		try (BaseCatalog catalog = catalogObject.getCatalog()) {

			catalog.open();

			catalog.sinkStream(
				catalogObject.getObjectPath(), in.getOutputTable(), catalogObject.getParams(), getMLEnvironmentId()
			);
		}

		return this;
	}
}
