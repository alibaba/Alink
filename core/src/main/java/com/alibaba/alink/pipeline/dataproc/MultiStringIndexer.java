package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.params.dataproc.MultiStringIndexerPredictParams;
import com.alibaba.alink.params.dataproc.MultiStringIndexerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Encode several columns of strings to bigint type indices. The indices are consecutive bigint type
 * that start from 0. Non-string columns are first converted to strings and then encoded. Each columns
 * are encoded separately.
 * <p>
 * <p>Several string order type is supported, including:
 * <ol>
 * <li>random</li>
 * <li>frequency_asc</li>
 * <li>frequency_desc</li>
 * <li>alphabet_asc</li>
 * <li>alphabet_desc</li>
 * </ol>
 */
@NameCn("多字段字符串编码")
@NameEn("Multiple String Indexer")
public class MultiStringIndexer extends Trainer <MultiStringIndexer, MultiStringIndexerModel> implements
	MultiStringIndexerTrainParams <MultiStringIndexer>,
	MultiStringIndexerPredictParams <MultiStringIndexer> {

	private static final long serialVersionUID = 8719095455771046412L;

	public MultiStringIndexer() {
		this(new Params());
	}

	public MultiStringIndexer(Params params) {
		super(params);
	}

}
