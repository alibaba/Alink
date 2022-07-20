package com.alibaba.alink.operator.common.similarity.dataConverter;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.distance.FastCategoricalDistance;
import com.alibaba.alink.operator.common.distance.LevenshteinDistance;
import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.modeldata.StringModelData;
import com.alibaba.alink.operator.common.similarity.similarity.Cosine;
import com.alibaba.alink.operator.common.similarity.similarity.LevenshteinSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.LongestCommonSubsequence;
import com.alibaba.alink.operator.common.similarity.similarity.LongestCommonSubsequenceSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.SubsequenceKernelSimilarity;
import com.alibaba.alink.params.similarity.StringTextNearestNeighborTrainParams;

import java.util.List;

public class StringModelDataConverter extends NearestNeighborDataConverter <StringModelData> {
	private static final long serialVersionUID = 8761170480003926433L;
	public static ParamInfo <Boolean> TEXT = ParamInfoFactory
		.createParamInfo("text", Boolean.class)
		.setDescription("text")
		.setHasDefaultValue(false)
		.build();

	private static int ROW_SIZE = 3;
	private static int ID_INDEX = 0;
	private static int DATA_INDEX = 1;
	private static int LABEL_INDEX = 2;

	public StringModelDataConverter() {
		this.rowSize = ROW_SIZE;
	}

	@Override
	public TableSchema getModelDataSchema() {
		return new TableSchema(new String[] {"ID", "DATA", "LABEL"},
			new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING});
	}

	@Override
	public StringModelData loadModelData(List <Row> list) {
		Sample[] dictSamples = new Sample[list.size() - 1];
		int cnt = 0;
		for (Row row : list) {
			if (row.getField(ID_INDEX) != null) {
				Object id = row.getField(ID_INDEX);
				String str = (String) row.getField(DATA_INDEX);
				String label = (String) row.getField(LABEL_INDEX);
				dictSamples[cnt++] = new Sample <Double>(str, Row.of(id),
					null == label ? null : Double.valueOf(label));
			}
		}
		return new StringModelData(dictSamples, initSimilarity(meta), meta.get(TEXT));
	}

	@Override
	public DataSet <Row> buildIndex(BatchOperator in, Params params) {
		DataSet <Row> dataSet = in.getDataSet();
		FastCategoricalDistance similarity = initSimilarity(params);

		DataSet <Row> index = dataSet.map(new MapFunction <Row, Row>() {
			private static final long serialVersionUID = -600268964767461036L;

			@Override
			public Row map(Row value) throws Exception {
				boolean text = params.get(TEXT);
				Sample sample = similarity.prepareSample((String) value.getField(1), text);
				Row row = new Row(ROW_SIZE);
				row.setField(ID_INDEX, value.getField(0).toString());
				if (sample.getLabel() != null) {
					row.setField(LABEL_INDEX, sample.getLabel().toString());
				}
				row.setField(DATA_INDEX, sample.getStr());
				return row;
			}
		});

		return index
			.mapPartition(new RichMapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = -1078356373351365760L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out)
					throws Exception {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = params;
					}
					new StringModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			})
			.name("build_model");
	}

	private static FastCategoricalDistance initSimilarity(Params params) {
		switch (params.get(StringTextNearestNeighborTrainParams.METRIC)) {
			case LEVENSHTEIN: {
				return new LevenshteinDistance();
			}
			case LEVENSHTEIN_SIM: {
				return new LevenshteinSimilarity();
			}
			case LCS: {
				return new LongestCommonSubsequence();
			}
			case LCS_SIM: {
				return new LongestCommonSubsequenceSimilarity();
			}
			case SSK: {
				return new SubsequenceKernelSimilarity(params.get(StringTextNearestNeighborTrainParams.WINDOW_SIZE),
					params.get(StringTextNearestNeighborTrainParams.LAMBDA));
			}
			case COSINE: {
				return new Cosine(params.get(StringTextNearestNeighborTrainParams.WINDOW_SIZE));
			}
			default: {
				throw new AkUnsupportedOperationException("unsupported distance type:"
					+ params.get(StringTextNearestNeighborTrainParams.METRIC).toString());
			}
		}
	}
}
