package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
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
	private final DiscreteMapperBuilder mapperBuilder;

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
					cutsArray[i][j] = Double.parseDouble(points[j].trim());
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
					throw new AkUnsupportedOperationException("Unsupported now.");
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
			mapperBuilder.discretizers[i] = new QuantileDiscretizerModelMapper.DoubleNumericQuantileDiscretizer(
				bounds,
				params.get(BucketizerParams.LEFT_OPEN),
				boundsIndex,
				binCount,
				false
			);
		}

		mapperBuilder.setAssembledVectorSize();
	}

	@Override
	public void open() {
		this.mapperBuilder.open();
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		mapperBuilder.map(selection, result);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		DiscreteMapperBuilder discreteMapperBuilder = new DiscreteMapperBuilder(params, dataSchema);

		return Tuple4.of(discreteMapperBuilder.paramsBuilder.selectedCols,
			discreteMapperBuilder.paramsBuilder.resultCols,
			discreteMapperBuilder.paramsBuilder.resultColTypes,
			discreteMapperBuilder.paramsBuilder.reservedCols
		);
	}
}
