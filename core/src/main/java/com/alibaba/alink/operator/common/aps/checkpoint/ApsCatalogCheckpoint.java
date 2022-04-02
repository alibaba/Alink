package com.alibaba.alink.operator.common.aps.checkpoint;

import com.alibaba.alink.common.io.catalog.BaseCatalog;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.CatalogSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CatalogSourceBatchOp;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.params.io.HasCatalogObject.CatalogObject;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.catalog.ObjectPath;

public class ApsCatalogCheckpoint extends ApsCheckpoint {
    private final BaseCatalog catalog;
    private final String database;

    public ApsCatalogCheckpoint(BaseCatalog catalog) {
        this(catalog, null);
    }

    public ApsCatalogCheckpoint(BaseCatalog catalog, String database) {
        this.catalog = catalog;
        this.database = database == null ? catalog.getDefaultDatabase() : database;
    }

    @Override
    public void write(BatchOperator<?> operator, String identity, Long mlEnvId, Params params) {
        operator.link(
            new CatalogSinkBatchOp()
                .setCatalogObject(
                    new CatalogObject(
                        catalog,
                        new ObjectPath(database, identity),
                        params
                    )
                )
                .setMLEnvironmentId(mlEnvId)
        );
    }

    @Override
    public BatchOperator <?> read(String identity, Long mlEnvId, Params params) {
        return new CatalogSourceBatchOp()
            .setCatalogObject(
                new CatalogObject(
                    catalog,
                    new ObjectPath(database, identity),
                    params
                )
            )
            .setMLEnvironmentId(mlEnvId);
    }
}
