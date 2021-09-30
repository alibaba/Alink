package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.ModelMapper;

import com.alibaba.alink.params.dataproc.HasHandleInvalid.HandleInvalid;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoeAndIndex.Encode;
import com.alibaba.alink.params.feature.MultiHotPredictParams;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transform a labeled data to a sparse vector with the multi-hot model.
 */
public class MultiHotModelMapper extends ModelMapper {

	private static final long serialVersionUID = 7431062592310976413L;

	private MultiHotModelData model;
	private String[] selectedCols;
	private final HandleInvalid handleInvalid;
	private final Encode encode;
	private int offsetSize = 0;
	boolean enableElse = false;

	public MultiHotModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		this.handleInvalid = params.get(MultiHotPredictParams.HANDLE_INVALID);
		this.encode = params.get(MultiHotPredictParams.ENCODE);
		if (handleInvalid.equals(HandleInvalid.KEEP)) {
			offsetSize = 1;
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.model = new MultiHotModelDataConverter().load(modelRows);
		this.enableElse = this.model.getEnableElse(selectedCols);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(

		TableSchema modelSchema, TableSchema dataSchema, Params params) {
		String[] reservedCols = params.get(MultiHotPredictParams.RESERVED_COLS);
		if (reservedCols == null) {
			reservedCols = dataSchema.getFieldNames();
		}
		this.selectedCols = params.get(MultiHotPredictParams.SELECTED_COLS);

		String[] outputCols = params.get(MultiHotPredictParams.OUTPUT_COLS);
		TypeInformation <?>[] outputTypes = new TypeInformation <?>[outputCols.length];
		Arrays.fill(outputTypes, VectorTypes.SPARSE_VECTOR);
		return Tuple4.of(this.selectedCols, outputCols, outputTypes, reservedCols);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		if (encode.equals(Encode.ASSEMBLED_VECTOR)) {
			Tuple2 <Integer, int[]> indices = getIndicesAndSize(selection);
			double[] vals = new double[indices.f1.length];
			Arrays.fill(vals, 1.0);
			if (indices.f1.length != 0) {
				result.set(0, new SparseVector(indices.f0, indices.f1, vals));
			}
		} else if (encode.equals(Encode.VECTOR)) {
			for (int i = 0; i < selection.length(); ++i) {
				String str = (String) selection.get(i);
				Tuple2 <Integer, int[]> indices = getSingleIndicesAndSize(selectedCols[i], str);
				double[] vals = new double[indices.f1.length];
				Arrays.fill(vals, 1.0);
				if (indices.f1.length != 0) {
					result.set(i, new SparseVector(indices.f0, indices.f1, vals));
				}
			}
		}
	}

	public Tuple2 <Integer, int[]> getSingleIndicesAndSize(String colName, String str) {
		Set <Integer> set = new HashSet <>();
		Map <String, Tuple2 <Integer, Integer>> map = model.modelData.get(colName);

		if (str != null) {
			String[] content = str.split(model.delimiter);
			for (String val : content) {
				Tuple2 <Integer, Integer> t3 = map.get(val.trim());
				if (t3 != null) {
					if (t3.f0 != -1) {
						set.add(t3.f0);
					} else {
						set.add(map.size());
					}
				} else {
					switch (handleInvalid) {
						case KEEP:
							set.add(map.size() + (enableElse ? 1 : 0));
						case SKIP:
							continue;
						case ERROR:
							throw new RuntimeException("multi hot encoder err, key is not exist.");
					}
				}
			}
		}
		int[] indices = new int[set.size()];
		Iterator <Integer> iter = set.iterator();
		for (int i = 0; i < indices.length; ++i) {
			indices[i] = iter.next();
		}
		return Tuple2.of(map.size() + (enableElse ? 1 : 0) + offsetSize, indices);

	}

	public Tuple2 <Integer, int[]> getIndicesAndSize(SlicedSelectedSample selection) {
		Set <Integer> set = new HashSet <>();
		int cnt = 0;
		for (int i = 0; i < selection.length(); ++i) {
			String str = (String) selection.get(i);
			Tuple2 <Integer, int[]> t2 = getSingleIndicesAndSize(this.selectedCols[i], str);
			for (int j = 0; j < t2.f1.length; ++j) {
				set.add(cnt + t2.f1[j]);
			}
			cnt += t2.f0;
		}
		int[] indices = new int[set.size()];
		Iterator <Integer> iter = set.iterator();
		for (int i = 0; i < indices.length; ++i) {
			indices[i] = iter.next();
		}
		return Tuple2.of(cnt, indices);
	}
}

