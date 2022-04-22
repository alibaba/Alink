package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlierPredictStreamOp;
import com.alibaba.alink.params.outlier.DbscanDetectorParams;
import com.alibaba.alink.operator.common.outlier.DbscanModelDetector;

@NameCn("有模型的DBSCAN序列异常检测")
@NameEn("DBSCAN Series Outlier with Input Model")
public class DbscanModelOutlierPredictStreamOp
	extends BaseModelOutlierPredictStreamOp <DbscanModelOutlierPredictStreamOp>
	implements DbscanDetectorParams <DbscanModelOutlierPredictStreamOp> {

	public DbscanModelOutlierPredictStreamOp() {
		super(DbscanModelDetector::new, new Params());
	}

	public DbscanModelOutlierPredictStreamOp(Params params) {
		super(DbscanModelDetector::new, params);
	}

	public DbscanModelOutlierPredictStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public DbscanModelOutlierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, DbscanModelDetector::new, params);
	}
}
