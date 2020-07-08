package com.alibaba.alink.operator.stream.utils;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dataproc.UDTFParams;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

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
public class UDTFStreamOp extends StreamOperator<UDTFStreamOp>
    implements UDTFParams<UDTFStreamOp> {

    private TableFunction<?> func;

    public UDTFStreamOp() {
        super(null);
    }

    public UDTFStreamOp(Params params) {
        super(params);
    }

    public UDTFStreamOp setFunc(TableFunction<?> udf) {
        this.func = udf;
        return this;
    }

    public TableFunction<?> getFunc() {
        return this.func;
    }

    @Override
    public UDTFStreamOp linkFrom(StreamOperator<?>... inputs) {
        if (null == getFunc() && null == getFuncName()) {
            throw new IllegalArgumentException("A TableFunction or a registered function name must be set using setFunc or setFuncName.");
        }
        StreamOperator<?> in = checkAndGetFirst(inputs);
        String[] reservedCols = ObjectUtils.defaultIfNull(getReservedCols(), in.getColNames());

        StreamTableEnvironment tEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment();

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
