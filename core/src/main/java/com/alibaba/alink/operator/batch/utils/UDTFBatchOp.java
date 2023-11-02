package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.dataproc.UDTFParams;
import org.apache.commons.lang3.ObjectUtils;

/**
 * This class provides the UDTF feature which is similar with Flink user-defined table functions.
 * <p>
 * An instance of a class inheriting Flink TableFunction is provided.
 * The computation involves selectedCols and outputCols,
 * and reservedCols are columns kept from the input table.
 * <p>
 * Note that outputCols can have same names with the selectedCols.
 * outputCols can also have same names with the reservedCols,
 * and in that case the corresponding columns in the input table do not exist in the output table..
 * <p>
 * (https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/udfs.html#table-functions)
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("UDTF")
@NameEn("UDTF")
public class UDTFBatchOp extends BatchOperator <UDTFBatchOp>
	implements UDTFParams <UDTFBatchOp> {

	private static final long serialVersionUID = 7007323021840765107L;
	private TableFunction <?> func;

	public UDTFBatchOp() {
		this(null);
	}

	public UDTFBatchOp(Params params) {
		super(params);
	}

	public UDTFBatchOp setFunc(TableFunction <?> func) {
		this.func = func;
		return this;
	}

	public TableFunction <?> getFunc() {
		return this.func;
	}

	@Override
	public UDTFBatchOp linkFrom(BatchOperator <?>... inputs) {
		if (null == getFunc() && null == getFuncName()) {
			throw new AkIllegalOperatorParameterException(
				"A TableFunction or a registered function name must be set using setFunc or setFuncName.");
		}
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] reservedCols = ObjectUtils.defaultIfNull(getReservedCols(), in.getColNames());

		BatchTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getBatchTableEnvironment();

		String funcName = getFuncName();
		if (null == funcName) {
			funcName = UDFHelper.generateRandomFuncName();
			tEnv.registerFunction(funcName, func);
		}

		String clause = UDFHelper.generateUDTFClause(in.getOutputTable().toString(), funcName,
			getOutputCols(), getSelectedCols(), reservedCols);
		this.setOutputTable(tEnv.sqlQuery(clause));
		return this;
	}

}
