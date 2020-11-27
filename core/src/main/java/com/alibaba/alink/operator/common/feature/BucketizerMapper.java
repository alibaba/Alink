package com.alibaba.alink.operator.common.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper.DiscreteMapperBuilder;
import com.alibaba.alink.params.feature.BucketizerParams;

import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Bucketizer mapper.
 */
public class BucketizerMapper extends Mapper {
	private static final long serialVersionUID = -174160347441153948L;
	private DiscreteMapperBuilder mapperBuilder;

	public BucketizerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		mapperBuilder = new DiscreteMapperBuilder(params, dataSchema);

		double[][] cutsArray;
		//For Web, To delete when copy to ffa19
		if (params.contains(BucketizerParams.CUTS_ARRAY)) {
			cutsArray = params.get(BucketizerParams.CUTS_ARRAY);

		} else {
			String[] cutsArrayStr = params.get(BucketizerParams.CUTS_ARRAY_STR);
			cutsArray = new double[cutsArrayStr.length][];
			for (int i = 0; i < cutsArrayStr.length; i++) {
				String[] points = cutsArrayStr[i].split(":");
				cutsArray[i] = new double[points.length];
				for (int j = 0; j < points.length; j++) {
					cutsArray[i][j] = Double.valueOf(points[j].trim());
				}
			}
		}

		Preconditions.checkArgument(mapperBuilder.paramsBuilder.selectedCols.length == cutsArray.length,
			"The lengths of selectedCols and cusArray are not equal!");

		for (int i = 0; i < mapperBuilder.paramsBuilder.selectedCols.length; i++) {
			double[] cuts = cutsArray[i];
			Arrays.sort(cuts);
			int binCount = cuts.length + 1;

			switch (mapperBuilder.paramsBuilder.handleInvalidStrategy) {
				case KEEP:
					mapperBuilder.vectorSize.put(i, (long) binCount + 1);
					break;
				case SKIP:
				case ERROR:
					mapperBuilder.vectorSize.put(i, (long) binCount + 1);
					break;
				default:
					throw new UnsupportedOperationException("Unsupported now.");
			}

			if (mapperBuilder.paramsBuilder.dropLast) {
				mapperBuilder.dropIndex.put(i, (long) cuts.length - 1);
			}

			double[] bounds = new double[binCount + 1];
			bounds[0] = Double.NEGATIVE_INFINITY;
			System.arraycopy(cuts, 0, bounds, 1, binCount - 1);
			bounds[binCount] = Double.POSITIVE_INFINITY;

			int[] boundsIndex = IntStream.range(0, binCount + 2).toArray();
			boundsIndex[binCount] = binCount - 1;
			mapperBuilder.discretizers[i] = new QuantileDiscretizerModelMapper.DoubleNumericQuantileDiscretizer(bounds,
				params.get(BucketizerParams.LEFT_OPEN),
				boundsIndex,
				binCount,
				false);
		}
		mapperBuilder.setAssembledVectorSize();
	}

	@Override
	public TableSchema getOutputSchema() {
		return mapperBuilder.paramsBuilder.outputColsHelper.getResultSchema();
	}

	@Override
	public Row map(Row row) throws Exception {
		return mapperBuilder.map(row);
	}
}
