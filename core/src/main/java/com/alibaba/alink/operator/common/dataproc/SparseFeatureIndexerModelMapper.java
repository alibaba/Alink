package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalModelException;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.SISOModelMapper;
import com.alibaba.alink.params.dataproc.HasHandleDuplicateFeature;
import com.alibaba.alink.params.dataproc.SparseFeatureIndexerPredictParams;
import com.alibaba.alink.params.dataproc.SparseFeatureIndexerTrainParams;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparseFeatureIndexerModelMapper extends SISOModelMapper {
	private static final long serialVersionUID = 1781493175898726609L;
	private Map <String, Integer> mapper;
	private HasHandleDuplicateFeature.HandleDuplicate handleDuplicate;
	private String feaSplit;
	private String feaValueSplit;
	private boolean hasWeight;
	private Params meta;

	public SparseFeatureIndexerModelMapper(TableSchema modelSchema,
										   TableSchema dataSchema,
										   Params params) {
		super(modelSchema, dataSchema, params);
		handleDuplicate = params.get(SparseFeatureIndexerPredictParams.HANDLE_DUPLICATE_FEATURE);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		List <Tuple2 <String, Long>> model = new StringIndexerModelDataConverter().load(modelRows);
		this.mapper = new HashMap <>();
		for (Tuple2 <String, Long> record : model) {
			if (null == record.f1) {
				this.meta = Params.fromJson(record.f0);
				feaSplit = this.meta.get(SparseFeatureIndexerTrainParams.SPARSE_FEATURE_DELIMITER);
				feaValueSplit = this.meta.get(SparseFeatureIndexerTrainParams.KV_VAL_DELIMITER);
				hasWeight = this.meta.get(SparseFeatureIndexerTrainParams.HAS_VALUE);
			} else{
				if (record.f1 > Integer.MAX_VALUE) {
					throw new AkIllegalModelException("indexer id should less than Integer.MAX_VALUE. Set topN parameter"
						+ " in SparseFeatureIndexerTrainBatchOp");
				}
				this.mapper.put(record.f0, record.f1.intValue());
			}
		}
	}

	@Override
	protected TypeInformation <?> initPredResultColType() {
		return AlinkTypes.SPARSE_VECTOR;
	}

	@Override
	protected Object predictResult(Object input) throws Exception {
		if (null == input) {
			return null;
		}
		String feature = String.valueOf(input);
		if (feature.length() == 0) {
			return new SparseVector();
		}
		String[] feas = StringUtils.split(feature, this.feaSplit);
		HashMap<String, Integer> feaMap = new HashMap <>();
		List<Tuple2<Integer, String>> feaList = new ArrayList <>(feas.length);
		for (String fea : feas) {
			if (fea.length() == 0) {
				continue;
			}
			String[] feaAndValues = StringUtils.split(fea, this.feaValueSplit);
			String currentFea = fea;
			String currentValue = "1";
			if (hasWeight) {
				if (feaAndValues.length != 2 || feaAndValues[0].length() == 0 || feaAndValues[1].length() == 0) {
					continue;
				}
				currentFea = feaAndValues[0];
				currentValue = feaAndValues[1];
			}
			if (!this.mapper.containsKey(currentFea)) {
				continue;
			}
			int feaId = this.mapper.get(currentFea);
			if (feaMap.containsKey(currentFea)) {
				switch (this.handleDuplicate) {
					case FIRST:
						continue;
					case LAST:
						feaList.set(feaMap.get(currentFea), Tuple2.of(feaId, currentValue));
						continue;
					case ERROR:
						throw new AkIllegalDataException("Duplicate feature " + currentFea +
							" in sample " + feature);
				}
			} else {
				feaList.add(Tuple2.of(feaId, currentValue));
				feaMap.put(currentFea, feaList.size() - 1);
			}
		}
		SparseVector sv = new SparseVector();
		sv.setSize(mapper.size());
		for (Tuple2<Integer, String> t : feaList) {
			sv.set(t.f0.intValue(), Double.valueOf(t.f1));
		}
		return sv;
	}
}
