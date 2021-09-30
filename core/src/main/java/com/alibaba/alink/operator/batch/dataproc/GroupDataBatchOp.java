package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.sql.builtin.agg.MTableAgg;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.dataproc.GroupDataParams;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Transform MTable format recommendation to table format.
 */
public class GroupDataBatchOp extends BatchOperator <GroupDataBatchOp>
	implements GroupDataParams <GroupDataBatchOp> {

	private static final long serialVersionUID = 790348573681664909L;
	private String aggName;

	public GroupDataBatchOp() {
		this(null);
	}

	public GroupDataBatchOp(Params params) {
		super(params);
		aggName = "mtable_agg_" + UUID.randomUUID().toString().replace("-", "");

	}

	@Override
	public GroupDataBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = inputs[0];
		MTableAgg agg = new MTableAgg();

		String[] groupCols = this.getGroupCols();
		if (groupCols == null) {
			throw new RuntimeException("groupCols must be not empty.");
		}

		String[] zippedCols = this.getSelectedCols();

		if (zippedCols == null) {
			List <String> zippedColsList = new ArrayList <>();
			for (String col : in.getColNames()) {
				if (TableUtil.findColIndex(groupCols, col) == -1) {
					zippedColsList.add(col);
				}
			}
			zippedCols = zippedColsList.toArray(new String[0]);
		}

		TypeInformation[] zipColTypes = TableUtil.findColTypes(in.getSchema(), zippedCols);
		agg.setSchemaStr(CsvUtil.schema2SchemaStr(new TableSchema(zippedCols, zipColTypes)), null);
		agg.setDropLast(false);
		MLEnvironmentFactory.get(this.getMLEnvironmentId()).getBatchTableEnvironment().registerFunction(aggName, agg);
		String sql = TableUtil.columnsToSqlClause(groupCols) + ","
			+ aggName + "(" + TableUtil.columnsToSqlClause(zippedCols) + ") as " + this.getOutputCol();
		this.setOutputTable(in.groupBy(TableUtil.columnsToSqlClause(groupCols), sql).getOutputTable());
		return this;
	}
}
