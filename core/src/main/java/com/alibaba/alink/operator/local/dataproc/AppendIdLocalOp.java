package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.dataproc.AppendIdBatchParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;

/**
 * Append an id column to BatchOperator. the id can be DENSE or UNIQUE
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(PortType.DATA)})
@NameCn("添加id列")
public final class AppendIdLocalOp extends LocalOperator <AppendIdLocalOp>
	implements AppendIdBatchParams <AppendIdLocalOp> {
	public final static String appendIdColName = "append_id";
	public final static TypeInformation appendIdColType = BasicTypeInfo.LONG_TYPE_INFO;

	public AppendIdLocalOp() {
		super(null);
	}

	public AppendIdLocalOp(Params params) {
		super(params);
	}

	public static MTable appendId(MTable mt) {
		return AppendIdLocalOp.appendId(mt, AppendIdLocalOp.appendIdColName);
	}

	public static MTable appendId(MTable mt, String appendIdColName) {
		String[] colNames = ArrayUtils.add(mt.getColNames(), appendIdColName);
		TypeInformation <?>[] colTypes = ArrayUtils.add(mt.getColTypes(), appendIdColType);

		ArrayList <Row> list = new ArrayList <>();
		int m = mt.getNumCol();
		long k = 0;
		for (Row in : mt.getRows()) {
			Row out = new Row(m + 1);
			for (int i = 0; i < m; i++) {
				out.setField(i, in.getField(i));
			}
			out.setField(m, k);
			k += 1;
			list.add(out);
		}

		return new MTable(list, new TableSchema(colNames, colTypes));
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		checkOpSize(1, inputs);
		this.setOutputTable(appendId(inputs[0].getOutputTable(), getIdCol()));
	}

}
