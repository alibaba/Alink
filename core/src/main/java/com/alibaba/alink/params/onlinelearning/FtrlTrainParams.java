package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ModelStreamScanParams;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;

public interface FtrlTrainParams<T> extends WithParams <T>,
	HasLabelCol <T>,
	HasVectorColDefaultAsNull <T>,
	HasFeatureColsDefaultAsNull <T>,
	HasWithIntercept <T>,
	HasTimeIntervalDefaultAs1800 <T>,
	ModelStreamScanParams <T>,
	HasL1DefaultAs01 <T>,
	HasL2DefaultAs01 <T>,
	HasAlpha <T>,
	HasBeta <T> {

	@NameCn("Batch大小")
	@DescCn("表示单次ftrl单次迭代更新使用的样本数量，建议是并行度的整数倍.")
	ParamInfo <Integer> MINI_BATCH_SIZE = ParamInfoFactory
		.createParamInfo("miniBatchSize", Integer.class)
		.setDescription("mini batch size")
		.setHasDefaultValue(512)
		.build();

	default Integer getMiniBatchSize() {return get(MINI_BATCH_SIZE);}

	default T setMiniBatchSize(Integer value) {return set(MINI_BATCH_SIZE, value);}

	@NameCn("向量长度")
	@DescCn("向量的长度")
	ParamInfo <Integer> VECTOR_SIZE = ParamInfoFactory
		.createParamInfo("vectorSize", Integer.class)
		.setDescription("vector size of embedding")
		.setRequired()
		.setAlias(new String[] {"vectorSize", "inputDim"})
		.build();

	@Deprecated
	default Integer getVectorSize() {
		return get(VECTOR_SIZE);
	}

	@Deprecated
	default T setVectorSize(Integer value) {
		return set(VECTOR_SIZE, value);
	}
}
