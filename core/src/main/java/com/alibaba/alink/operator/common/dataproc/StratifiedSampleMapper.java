package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
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

		String sampleStr = this.params.get(StratifiedSampleParams.STRATA_RATIOS);
		this.sampleRatios = new HashMap <>();
		String[] sp1 = sampleStr.split(",");
		for (String sp : sp1) {
			String[] sp2 = sp.split(":");
			AkPreconditions.checkArgument(sp2.length == 2, "Invalid format for param ratios.");
			double ratio = Double.parseDouble(sp2[1]);
			AkPreconditions.checkArgument(ratio >= 0 && ratio <= 1, "Param ratios must be in range [0, 1].");
			this.sampleRatios.put(sp2[0], ratio);
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
		String key = String.valueOf(row.getField(strataColIdx));
		if (sampleRatios.containsKey(key)) {
			ratio = sampleRatios.get(key);
		} else {
			if (ratio < 0 || ratio > 1) {
				throw new AkIllegalArgumentException("Illegal ratio  for [" + key + "]. "
					+ "Please set proper values for ratio or ratios.");
			}
		}

		double random = Math.random();
		if (random < ratio) {
			output.collect(row);
		}
	}

}
