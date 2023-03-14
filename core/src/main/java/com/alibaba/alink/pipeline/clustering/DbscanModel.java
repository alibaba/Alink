package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanModelMapper;
import com.alibaba.alink.params.clustering.ClusteringPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by Dbscan.
 */
@NameCn("Dbscan模型")
public class DbscanModel extends MapModel <DbscanModel>
	implements ClusteringPredictParams <DbscanModel> {
	private static final long serialVersionUID = 7645298355641765772L;
	private Table clusterResult;
	private BatchOperator <?> coreItem;

	DbscanModel(Table clusterResult, BatchOperator <?> coreItem) {
		super(DbscanModelMapper::new, new Params());
		this.clusterResult = clusterResult;
		this.coreItem = coreItem;
		super.setModelData(coreItem);
	}

	public Table getClusterResult() {
		return clusterResult;
	}

	public BatchOperator <?> getCoreItem() {
		return coreItem;
	}

}
