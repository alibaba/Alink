package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.classification.KnnMapper;
import com.alibaba.alink.params.classification.KnnPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Knn classification model fitted by KnnClassifier.
 */
@NameCn("最近邻分类模型")
public class KnnClassificationModel extends MapModel <KnnClassificationModel>
	implements KnnPredictParams <KnnClassificationModel> {

	private static final long serialVersionUID = 1303892137143865652L;

	public KnnClassificationModel() {this(null);}

	public KnnClassificationModel(Params params) {
		super(KnnMapper::new, params);
	}

}
