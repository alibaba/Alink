package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.StratifiedSampleParams;

import java.util.HashMap;
import java.util.Map;

public class StratifiedSampleMapper extends FlatMapper {
	private static final long serialVersionUID = -3276484935413372979L;
	private double sampleRatio;
	private Map <String, Double> sampleRatios;
	private int strataColIdx;

	public StratifiedSampleMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		String strataColName = this.params.get(StratifiedSampleParams.STRATA_COL);

		this.sampleRatio = this.params.get(StratifiedSampleParams.STRATA_RATIO);

		if (sampleRatio <= 0) {
			String sampleStr = this.params.get(StratifiedSampleParams.STRATA_RATIOS);
			this.sampleRatios = new HashMap <>();
			String[] sp1 = sampleStr.split(",");
			for (String sp : sp1) {
				String[] sp2 = sp.split(":");
				if (sp2.length != 2) {
					throw new RuntimeException("kv format error.");
				}
				this.sampleRatios.put(sp2[0], Double.parseDouble(sp2[1]));
			}
		}
		if (sampleRatio <= 0 && (sampleRatios.isEmpty())) {
			throw new RuntimeException("sample ratio and sample ratios must exsit one.");
		}

		this.strataColIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), strataColName);
	}

	@Override
	public TableSchema getOutputSchema() {
		return this.getDataSchema();
	}

	@Override
	public void flatMap(Row row, Collector <Row> output) throws Exception {
		double ratio = sampleRatio;
		if (sampleRatios != null) {
			ratio = sampleRatios.get(String.valueOf(row.getField(strataColIdx)));
		}

		double random = Math.random();
		if (random < ratio) {
			output.collect(row);
		}
	}

}
