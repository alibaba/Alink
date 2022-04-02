package com.alibaba.alink.operator.common.nlp.bert;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.params.tensorflow.bert.HasLayer;
import com.alibaba.alink.params.tensorflow.bert.HasHiddenStatesCol;
import com.alibaba.alink.params.tensorflow.bert.HasLengthCol;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.testutil.AlinkTestBase;
import com.google.common.primitives.Floats;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static com.alibaba.alink.operator.common.nlp.bert.BertEmbeddingExtractorMapper.SEP_CHAR;

public class BertEmbeddingExtractorMapperTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		int totalLayers = 13;
		int length = 10;
		int embeddingSize = 768;
		long[] shape = new long[] {totalLayers, length, embeddingSize};
		int layer = -1;

		Random random = new Random();

		float[] buffer = new float[totalLayers * length * embeddingSize];
		for (int i = 0; i < buffer.length; i += 1) {
			buffer[i] = random.nextFloat();
		}

		Params params = new Params();
		params.set(HasLengthCol.LENGTH_COL, "length");
		params.set(HasHiddenStatesCol.HIDDEN_STATES_COL, "hidden_states");
		params.set(HasOutputCol.OUTPUT_COL, "embed");
		params.set(HasReservedColsDefaultAsNull.RESERVED_COLS, new String[] {"text"});
		params.set(HasLayer.LAYER, layer);

		TableSchema dataSchema = TableSchema.builder()
			.field("text", Types.STRING)
			.field("hidden_states", AlinkTypes.FLOAT_TENSOR)
			.field("length", AlinkTypes.INT_TENSOR)
			.build();

		BertEmbeddingExtractorMapper mapper = new BertEmbeddingExtractorMapper(dataSchema, params);
		mapper.open();
		Row result = mapper.map(Row.of("sequence builders",
			new FloatTensor(buffer).reshape(new Shape(shape)),
			new IntTensor(new int[] {length})));
		System.out.println(result);
		mapper.close();

		float[] embed = new float[embeddingSize];
		Arrays.fill(embed, 0);
		int[] p = new int[] {(int) (shape[0] + layer), 0, 0};
		for (p[1] = 0; p[1] < length; p[1] += 1) {
			for (p[2] = 0; p[2] < embeddingSize; p[2] += 1) {
				int index = BertEmbeddingExtractorMapper.calcIndex(p, shape);
				embed[p[2]] += buffer[index] / length;
			}
		}
		Assert.assertEquals(Floats.join(SEP_CHAR, embed), result.getField(1));
	}
}
