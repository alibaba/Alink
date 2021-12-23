package com.alibaba.alink.operator.common.nlp.bert;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.params.tensorflow.bert.HasLayer;
import com.alibaba.alink.params.tensorflow.bert.HasHiddenStatesCol;
import com.alibaba.alink.params.tensorflow.bert.HasLengthCol;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.google.common.primitives.Floats;

import java.util.Arrays;

public class BertEmbeddingExtractorMapper extends Mapper {

	public static final String SEP_CHAR = " ";

	int layer;

	public BertEmbeddingExtractorMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		layer = params.get(HasLayer.LAYER);
	}

	protected static int calcIndex(int[] p, long[] shape) {
		int index = 0;
		for (int i = 0; i < shape.length; i += 1) {
			if (i > 0) {
				index *= shape[i];
			}
			index += p[i];
		}
		return index;
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		int length = ((IntTensor) selection.get(0)).getInt(0);
		FloatTensor hiddenStates = (FloatTensor) selection.get(1);
		long[] shape = hiddenStates.shape();

		int embeddingSize = (int) shape[shape.length - 1];
		float[] embed = new float[embeddingSize];
		Arrays.fill(embed, 0);

		long[] p = new long[] {shape[0] + layer, 0, 0};
		for (p[1] = 0; p[1] < length; p[1] += 1) {
			for (p[2] = 0; p[2] < embeddingSize; p[2] += 1) {
				embed[(int) p[2]] += hiddenStates.getFloat(p) / length;
			}
		}

		// TODO: still return previous format to make it compatible
		result.set(0, Floats.join(SEP_CHAR, embed));
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		String lengthCol = params.get(HasLengthCol.LENGTH_COL);
		String hiddenStatesCol = params.get(HasHiddenStatesCol.HIDDEN_STATES_COL);
		String outputCol = params.get(HasOutputCol.OUTPUT_COL);
		String[] reservedCols = params.get(HasReservedColsDefaultAsNull.RESERVED_COLS);
		return Tuple4.of(
			new String[] {lengthCol, hiddenStatesCol},
			new String[] {outputCol},
			new TypeInformation <?>[] {Types.STRING},
			reservedCols
		);
	}
}
