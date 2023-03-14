package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.params.dataproc.StringIndexerPredictParams;
import com.alibaba.alink.params.dataproc.StringIndexerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Encode one column of strings to bigint type indices.
 * The indices are consecutive bigint type that start from 0.
 * Non-string columns are first converted to strings and then encoded.
 * <p>
 * Several string order type is supported, including:
 * <ol>
 * <li>random</li>
 * <li>frequency_asc</li>
 * <li>frequency_desc</li>
 * <li>alphabet_asc</li>
 * <li>alphabet_desc</li>
 * </ol>
 */
@NameEn("String Indexer")
@NameCn("字符串编码")
public class StringIndexer extends Trainer <StringIndexer, StringIndexerModel> implements
	StringIndexerTrainParams <StringIndexer>,
	StringIndexerPredictParams <StringIndexer> {

	private static final long serialVersionUID = -5088740733118669048L;

	public StringIndexer() {
		super();
	}

	public StringIndexer(Params params) {
		super(params);
	}

}
