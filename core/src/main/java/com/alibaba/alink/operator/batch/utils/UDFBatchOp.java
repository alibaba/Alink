package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.dataproc.UDFParams;
import org.apache.commons.lang3.ObjectUtils;

/**
 * This class provides the UDF feature which is similar with Flink user-defined scalar functions.
 * <p>
 * An instance of a class inheriting Flink ScalarFunction is provided.
 * The computation involves selectedCols and outputCol,
 * and reservedCols are columns kept from the input table.
 * <p>
 * Note that outputCol can have same names with the selectedCols.
 * outputCol can also have same names with the reservedCols,
 * and in that case the corresponding columns in the input table do not exist in the output table..
 * <p>
 * (https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/udfs.html#scalar-functions)
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("UDF")
public class UDFBatchOp extends BatchOperator <UDFBatchOp>
	implements UDFParams <UDFBatchOp> {

	private static final long serialVersionUID = 2732123813882789130L;
	private ScalarFunction func;

	public UDFBatchOp() {
		this(null);
	}

	public UDFBatchOp(Params params) {
		super(params);
	}

	public UDFBatchOp setFunc(ScalarFunction udf) {
		this.func = udf;
		return this;
	}

	public ScalarFunction getFunc() {
		return this.func;
	}

	@Override
	public UDFBatchOp linkFrom(BatchOperator <?>... inputs) {
		if (null == getFunc() && null == getFuncName()) {
			throw new AkIllegalOperatorParameterException(
				"A ScalarFunction or a registered function name must be set using setFunc or setFuncName.");
		}
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] reservedCols = ObjectUtils.defaultIfNull(getReservedCols(), in.getColNames());

		BatchTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getBatchTableEnvironment();

		String funcName = getFuncName();
		if (null == funcName) {
			funcName = UDFHelper.generateRandomFuncName();
			tEnv.registerFunction(funcName, func);
		}

		String clause = UDFHelper.generateUDFClause(in.getOutputTable().toString(), funcName,
			getOutputCol(), getSelectedCols(), reservedCols);
		setOutputTable(tEnv.sqlQuery(clause));
		return this;
	}

}
