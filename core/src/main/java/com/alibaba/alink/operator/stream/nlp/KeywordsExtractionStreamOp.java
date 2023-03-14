package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.nlp.KeywordsExtractionMap;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.nlp.KeywordsExtractionStreamParams;

/**
 * Automatically identify in a text a set of terms that best describe the document based on TextRank.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("关键词抽取")
@NameEn("Keywords extraction")
public final class KeywordsExtractionStreamOp extends StreamOperator <KeywordsExtractionStreamOp>
	implements KeywordsExtractionStreamParams <KeywordsExtractionStreamOp> {
	private static final long serialVersionUID = 7089771952234251214L;

	/**
	 * default constructor.
	 */
	public KeywordsExtractionStreamOp() {
		super(null);
	}

	public KeywordsExtractionStreamOp(Params params) {
		super(params);
	}

	@Override
	public KeywordsExtractionStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		String selectedColName = this.getSelectedCol();
		int textColIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), selectedColName);
		String outputColName = this.getOutputCol();
		if (null == outputColName) {
			outputColName = selectedColName;
		}
		OutputColsHelper outputColsHelper = new OutputColsHelper(in.getSchema(), new String[] {outputColName},
			new TypeInformation[] {org.apache.flink.table.api.Types.STRING()}, in.getColNames());

		DataStream <Row> res = in.getDataStream()
			.map(new KeywordsExtractionMap(this.getParams(), textColIndex, outputColsHelper));

		// Set the output into table.
		this.setOutput(res, outputColsHelper.getResultSchema());
		return this;
	}
}
