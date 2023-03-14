package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.dataproc.SplitParams;

import java.util.ArrayList;
import java.util.Random;

/**
 * Split a dataset into two parts.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@NameCn("数据拆分")
@NameEn("Data Splitting")
public class SplitLocalOp extends LocalOperator <SplitLocalOp>
	implements SplitParams <SplitLocalOp> {

	public SplitLocalOp() {
		this(new Params());
	}

	public SplitLocalOp(Params params) {
		super(params);
	}

	@Override
	public SplitLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		Double fraction = getFraction();
		Integer seed = getRandomSeed();
				Random rand = (null==seed)?new Random(201706):new Random(seed);
		ArrayList<Row> train = new ArrayList <>();
		ArrayList<Row> test = new ArrayList <>();
		for(Row row:in.getOutputTable().getRows()){
			if(rand.nextDouble()<=fraction) {
				train.add(row);
			}else{
				test.add(row);
			}
		}

		this.setOutputTable(new MTable(train, in.getSchema()));
		this.setSideOutputTables(new MTable[]{new MTable(test, in.getSchema())});
		return this;
	}
}
