package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
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
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@NameCn("类型转换")
@NameEn("Type Converter")
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
			AkPreconditions.checkArgument(colIndice >= 0, "Can't find input column.");
			if (colTypes[colIndice].equals(AlinkTypes.VECTOR) ||
				colTypes[colIndice].equals(AlinkTypes.DENSE_VECTOR) ||
				colTypes[colIndice].equals(AlinkTypes.SPARSE_VECTOR)) {
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
			throw new AkIllegalDataException("Input data should have at least 1 column.");
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
					case "decimal":
						outTypes[i] = Types.DECIMAL();
						type = "DECIMAL";
						break;
					default:
						throw new AkUnsupportedOperationException("Not support type:" + this.newType);
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
				throw new AkIllegalOperatorParameterException("col:" + colName + " does not exist.");
			}
		}

		DataSet <Row> res = in.select(selectBuilder.toString()).getDataSet();
		this.setOutput(res, in.getColNames(), outTypes);
		return this;
	}
}
