package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.params.feature.ExclusiveFeatureBundlePredictParams;
import scala.Array;

import java.util.List;

public class ExclusiveFeatureBundleModelMapper extends ModelMapper {
	private FeatureBundles bundles;

	public ExclusiveFeatureBundleModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		Row row = bundles.map(selection.get(0));
		for (int i = 0; i < result.length(); i++) {
			result.set(i, row.getField(i));
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		int n = modelSchema.getFieldNames().length;
		String[] resultColNames = new String[n - 2];
		TypeInformation <?>[] resultColTypes = new TypeInformation[n - 2];

		Array.copy(modelSchema.getFieldNames(), 2, resultColNames, 0, n - 2);
		Array.copy(modelSchema.getFieldTypes(), 2, resultColTypes, 0, n - 2);

		return Tuple4.of(
			new String[] {params.get(ExclusiveFeatureBundlePredictParams.SPARSE_VECTOR_COL)},
			resultColNames,
			resultColTypes,
			params.get(ExclusiveFeatureBundlePredictParams.RESERVED_COLS)
		);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		ExclusiveFeatureBundleModelDataConverter converter = new ExclusiveFeatureBundleModelDataConverter();
		bundles = converter.load(modelRows);
	}
}
