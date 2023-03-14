package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkColumnNotFoundException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.VectorSerializeStreamOp;
import com.alibaba.alink.params.dataproc.TypeConvertParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Convert column datatype
 * TypeConvertStreamOp can convert some columns to the same datatype in one time.
 */

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@ParamSelectColumnSpec(name = "selectedCols", portIndices = 0)
@NameCn("类型转换")
@NameEn("Type convert")
public final class TypeConvertStreamOp extends StreamOperator <TypeConvertStreamOp>
	implements TypeConvertParams <TypeConvertStreamOp> {

	private static final long serialVersionUID = -6401780637549331163L;
	private String[] selectedColNames;
	private String newType;

	public TypeConvertStreamOp() {
		this(new Params());
	}

	public TypeConvertStreamOp(Params params) {
		super(params);
	}

	private static String wrapColumnName(String colName) {
		return "`" + colName + "`";
	}

	private static StreamOperator preprocess(StreamOperator in, String[] selectedColNames) {
		int[] colIndices = TableUtil.findColIndicesWithAssertAndHint(in.getColNames(), selectedColNames);
		TypeInformation[] colTypes = in.getColTypes();
		List <Integer> vectorCols = new ArrayList <>();
		for (int colIndice : colIndices) {
			if (colTypes[colIndice].equals(AlinkTypes.VECTOR) ||
				colTypes[colIndice].equals(AlinkTypes.DENSE_VECTOR) ||
				colTypes[colIndice].equals(AlinkTypes.SPARSE_VECTOR)) {
				vectorCols.add(colIndice);
			}
		}
		if (vectorCols.size() > 0) {
			in = in.link(new VectorSerializeStreamOp().setMLEnvironmentId(in.getMLEnvironmentId()));
		}
		return in;
	}

	@Override
	public TypeConvertStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
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
			throw new AkIllegalOperatorParameterException("Input data should have at least 1 column.");
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
				throw new AkColumnNotFoundException("col:" + colName + " does not exist.");
			}
		}

		//            System.out.println(selectBuilder.toString());
		DataStream <Row> res = in.select(selectBuilder.toString()).getDataStream();

		this.setOutput(res, in.getColNames(), outTypes);
		return this;
	}
}
