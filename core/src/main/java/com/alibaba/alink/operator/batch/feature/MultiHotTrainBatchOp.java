package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.MultiHotModelData;
import com.alibaba.alink.operator.common.feature.MultiHotModelDataConverter;
import com.alibaba.alink.operator.common.feature.MultiHotModelInfo;
import com.alibaba.alink.params.feature.MultiHotTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.HashMap;
import java.util.Map;

/**
 * Multi hot encoding train process.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("多热编码训练")
@NameEn("Multi Hot Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.MultiHotEncoder")
public final class MultiHotTrainBatchOp extends BatchOperator <MultiHotTrainBatchOp>
	implements MultiHotTrainParams <MultiHotTrainBatchOp>,
	WithModelInfoBatchOp <MultiHotModelInfo, MultiHotTrainBatchOp, MultiHotModelInfoBatchOp> {
	private static final long serialVersionUID = -5063129126354049743L;

	public MultiHotTrainBatchOp() {
		this(null);
	}

	public MultiHotTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public MultiHotTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		final String[] selectedCols = getSelectedCols();
		final String[] inputNames = in.getColNames();
		final int[] selectedIndices = TableUtil.findColIndices(inputNames, selectedCols);
		final int thresholds = getDiscreteThresholds();
		final Integer[] thresholdsArray = getDiscreteThresholdsArray();
		final String delimiter = getDelimiter();

		DataSet <Row> modelRows = in.getDataSet().mapPartition(
			new MapPartitionFunction <Row, Tuple2 <String, Map <String, Integer>>>() {
				@Override
				public void mapPartition(Iterable <Row> values, Collector <Tuple2 <String, Map <String, Integer>>> out)
					throws Exception {

					Map <String, Map <String, Integer>> maps = new HashMap <>();
					for (int i = 0; i < selectedCols.length; ++i) {
						maps.put(selectedCols[i], new HashMap <>());
					}
					for (Row row : values) {
						for (int i = 0; i < selectedCols.length; ++i) {
							String val = (String) row.getField(selectedIndices[i]);
							if (val == null) {
								continue;
							}
							String[] vals = val.split(delimiter);
							Map <String, Integer> map = maps.get(selectedCols[i]);
							for (String str : vals) {
								str = str.trim();
								if (map.containsKey(str)) {
									map.replace(str, map.get(str) + 1);
								} else {
									map.put(str, 1);
								}
							}
						}
					}
					for (int i = 0; i < selectedCols.length; ++i) {
						out.collect(Tuple2.of(selectedCols[i], maps.get(selectedCols[i])));
					}
				}
			}).groupBy(0).reduceGroup(
			new GroupReduceFunction <Tuple2 <String, Map <String, Integer>>, Tuple2 <String, Map <String, Integer>>>() {
				@Override
				public void reduce(Iterable <Tuple2 <String, Map <String, Integer>>> values,
								   Collector <Tuple2 <String, Map <String, Integer>>> out)
					throws Exception {
					String key = null;
					Map <String, Integer> map = new HashMap <>();
					for (Tuple2 <String, Map <String, Integer>> t2 : values) {
						key = t2.f0;
						for (String val : t2.f1.keySet()) {
							if (map.containsKey(val)) {
								map.replace(val, map.get(val) + t2.f1.get(val));
							} else {
								map.put(val, t2.f1.get(val));
							}
						}
					}
					out.collect(Tuple2.of(key, map));
				}
			}).reduceGroup(new GroupReduceFunction <Tuple2 <String, Map <String, Integer>>, Row>() {
			@Override
			public void reduce(Iterable <Tuple2 <String, Map <String, Integer>>> values, Collector <Row> out)
				throws Exception {
				Map <String, Map <String, Tuple2 <Integer, Integer>>> modeldata = new HashMap <>();
				for (Tuple2 <String, Map <String, Integer>> t2 : values) {
					int encoderIdx = TableUtil.findColIndex(inputNames, t2.f0);
					Map <String, Tuple2 <Integer, Integer>> mapping = new HashMap <>();
					int cnt = 0;
					for (String key : t2.f1.keySet()) {
						if (thresholds > 0) {
							if (t2.f1.get(key) >= thresholds) {
								mapping.put(key, Tuple2.of(cnt++, t2.f1.get(key)));
							} else {
								mapping.put(key, Tuple2.of(-1, t2.f1.get(key)));
							}
						} else if (thresholdsArray != null) {
							if (t2.f1.get(key) >= thresholdsArray[encoderIdx]) {
								mapping.put(key, Tuple2.of(cnt++, t2.f1.get(key)));
							} else {
								mapping.put(key, Tuple2.of(-1, t2.f1.get(key)));
							}
						} else {
							mapping.put(key, Tuple2.of(cnt++, t2.f1.get(key)));
						}
					}

					modeldata.put(t2.f0, mapping);
				}

				new MultiHotModelDataConverter().save(new MultiHotModelData(modeldata, delimiter), out);
			}
		});

		this.setOutput(modelRows, new MultiHotModelDataConverter().getModelSchema());

		return this;
	}

	@Override
	public MultiHotModelInfoBatchOp getModelInfoBatchOp() {
		return new MultiHotModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
