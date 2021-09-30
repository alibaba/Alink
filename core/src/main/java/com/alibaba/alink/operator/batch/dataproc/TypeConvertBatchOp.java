package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.VectorSerializeBatchOp;
import com.alibaba.alink.params.dataproc.TypeConvertParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Convert column datatype
 * TypeConvertBatchOp can convert some columns to the same datatype in one time.
 */

public final class TypeConvertBatchOp extends BatchOperator <TypeConvertBatchOp>
	implements TypeConvertParams <TypeConvertBatchOp> {

	private static final long serialVersionUID = 1141715577188721000L;
	private String[] selectedColNames;
	private String newType;

	public TypeConvertBatchOp() {
		this(new Params());
	}

	public TypeConvertBatchOp(Params params) {
		super(params);
	}

	public TypeConvertBatchOp(String[] selectedColNames, String newType) {
		this(new Params().set(SELECTED_COLS, selectedColNames)
			.set(TARGET_TYPE, TargetType.valueOf(newType.toUpperCase())));
	}

	private static String wrapColumnName(String colName) {
		return "`" + colName + "`";
	}

	private static BatchOperator preprocess(BatchOperator in, String[] selectedColNames) {
		int[] colIndices = TableUtil.findColIndices(in.getColNames(), selectedColNames);
		TypeInformation[] colTypes = in.getColTypes();
		List <Integer> vectorCols = new ArrayList <>();
		for (int colIndice : colIndices) {
			Preconditions.checkArgument(colIndice >= 0, "Can't find input column.");
			if (colTypes[colIndice].equals(VectorTypes.VECTOR) ||
				colTypes[colIndice].equals(VectorTypes.DENSE_VECTOR) ||
				colTypes[colIndice].equals(VectorTypes.SPARSE_VECTOR)) {
				vectorCols.add(colIndice);
			}
		}
		if (vectorCols.size() > 0) {
			in = in.link(new VectorSerializeBatchOp().setMLEnvironmentId(in.getMLEnvironmentId()));
		}
		return in;
	}

	@Override
	public TypeConvertBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		this.selectedColNames = getSelectedCols();
		this.newType = getTargetType().toString();

		if (this.newType.equalsIgnoreCase("varchar") || this.newType.equalsIgnoreCase("string")) {
			in = preprocess(in, selectedColNames);
		}

		TableSchema schema = in.getSchema();
		if (this.selectedColNames == null || this.selectedColNames.length == 0) {
			this.selectedColNames = schema.getFieldNames();
		}

		int colCount = this.selectedColNames.length;
		int allColCount = schema.getFieldNames().length;
		if (colCount == 0) {
			throw new RuntimeException("Input data should have at least 1 column.");
		}

		Set <String> colSet = new HashSet <>();
		for (int i = 0; i < colCount; ++i) {
			colSet.add(this.selectedColNames[i]);
		}
		Map <String, Boolean> colExist = new HashMap <>();
		for (int i = 0; i < colCount; ++i) {
			colExist.put(this.selectedColNames[i], false);
		}

		StringBuilder selectBuilder = new StringBuilder();
		TypeInformation <?>[] outTypes = new TypeInformation <?>[allColCount];
		for (int i = 0; i < allColCount; ++i) {
			outTypes[i] = schema.getFieldTypes()[i];
			String colName = schema.getFieldName(i).get();
			if (!colSet.contains(colName)) {
				if (i == 0) {
					selectBuilder.append(wrapColumnName(colName));
				} else {
					selectBuilder.append(",").append(wrapColumnName(colName));
				}
			} else {
				String type = null;
				switch (this.newType.toLowerCase()) {
					case "int":
					case "integer":
						outTypes[i] = Types.INT();
						type = outTypes[i].toString().toUpperCase();
						break;
					case "long":
					case "bigint":
						outTypes[i] = Types.LONG();
						type = "BIGINT";
						break;
					case "float":
						outTypes[i] = Types.FLOAT();
						type = outTypes[i].toString().toUpperCase();
						break;
					case "double":
						outTypes[i] = Types.DOUBLE();
						type = outTypes[i].toString().toUpperCase();
						break;
					case "boolean":
						outTypes[i] = Types.BOOLEAN();
						type = outTypes[i].toString().toUpperCase();
						break;
					case "varchar":
					case "string":
						outTypes[i] = Types.STRING();
						type = "VARCHAR";
						break;
					default:
						throw new RuntimeException("Not support type:" + this.newType);
				}

				if (i != 0) {
					selectBuilder.append(",");
				}

				if (schema.getFieldTypes()[i].equals(Types.BOOLEAN())
					&& !this.newType.equalsIgnoreCase("string")) {
					/**
					 * flink sql dose not support cast boolean to another numerical type.
					 * cast boolean to another type using case when.
					 * boolean -> numerical type
					 * true -> 1
					 * false -> 0
					 */
					selectBuilder
						.append("cast(case when ")
						.append(wrapColumnName(colName))
						.append(" then 1 else 0 end as ")
						.append(type)
						.append(") as ")
						.append(wrapColumnName(colName));
				} else {
					selectBuilder
						.append("cast(")
						.append(wrapColumnName(colName))
						.append(" as ")
						.append(type)
						.append(") as ")
						.append(wrapColumnName(colName));
				}

				colExist.put(colName, true);
			}
		}

		for (String colName : colExist.keySet()) {
			if (!colExist.get(colName)) {
				throw new RuntimeException("col:" + colName + " does not exist.");
			}
		}

		DataSet <Row> res = in.select(selectBuilder.toString()).getDataSet();
		this.setOutput(res, in.getColNames(), outTypes);
		return this;
	}
}
