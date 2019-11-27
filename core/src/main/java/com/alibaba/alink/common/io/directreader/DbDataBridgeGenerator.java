package com.alibaba.alink.common.io.directreader;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.DBSinkBatchOp;
import com.alibaba.alink.params.io.HasIoName;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

@DataBridgeGeneratorPolicy(policy = "db")
public class DbDataBridgeGenerator implements DataBridgeGenerator {
	private final static ParamInfo<String> NAME_PREFIX = ParamInfoFactory
		.createParamInfo("name.prefix", String.class)
		.setDescription("table name prefix")
		.build();

	@Override
	public DbDataBridge generate(BatchOperator batchOperator, Params dbParams) {
		String tableName = genTmpTableName(dbParams.get(NAME_PREFIX));

		try {
			String dbClsName = dbParams.get(HasIoName.IO_NAME);
			dbParams = dbParams.set(AnnotationUtils.tableAliasParamKey(dbClsName), tableName);

			BaseDB db = BaseDB.of(dbParams);

			DBSinkBatchOp dbSinkBatchOp = new DBSinkBatchOp(db, tableName);

			batchOperator.linkTo(dbSinkBatchOp);

			TableNeed2Collection.getCollectionClient().insertTable(dbParams);

			BatchOperator.getExecutionEnvironmentFromOps(batchOperator).execute();

			return db.initConnector(
				new DbDataBridge().setDbParams(dbParams)
			);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static synchronized String genTmpTableName(String prefix) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		if (prefix == null) {
			prefix = "tmp_alink";
		}

		return (prefix + "_" + df.format(new Date()) + "_" + UUID.randomUUID()).replace('-', '_');
	}
}
