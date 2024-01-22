package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.nlp.DocWordCountParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.alink.operator.common.nlp.WordCountUtil.COUNT_COL_NAME;
import static com.alibaba.alink.operator.common.nlp.WordCountUtil.WORD_COL_NAME;

/**
 * calculate doc word count.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "docIdCol")
@ParamSelectColumnSpec(name = "contentCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本词频统计")
public final class DocWordCountLocalOp extends LocalOperator <DocWordCountLocalOp>
	implements DocWordCountParams <DocWordCountLocalOp> {

	public DocWordCountLocalOp() {
		this(null);
	}

	public DocWordCountLocalOp(Params parameters) {
		super(parameters);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String wordDelimiter = this.getWordDelimiter();
		int indexContent = TableUtil.findColIndexWithAssert(in.getSchema(), this.getContentCol());
		int indexDocId = TableUtil.findColIndexWithAssert(in.getSchema(), this.getDocIdCol());

		ArrayList <Row> list = new ArrayList <>();
		for (Row row : in.getOutputTable().getRows()) {
			Object docId = row.getField(indexDocId);
			String content = row.getField(indexContent).toString();
			if (null == content || content.length() == 0) {
				continue;
			}

			HashMap <String, Long> map = new HashMap <>(0);
			for (String word : content.split(wordDelimiter)) {
				if (word.length() > 0) {
					map.merge(word, 1L, Long::sum);
				}
			}

			for (Map.Entry <String, Long> entry : map.entrySet()) {
				list.add(Row.of(docId, entry.getKey(), entry.getValue()));
			}
		}

		TableSchema schema = new TableSchema(
			new String[] {this.getDocIdCol(), WORD_COL_NAME, COUNT_COL_NAME},
			new TypeInformation <?>[] {in.getSchema().getFieldTypes()[indexDocId], AlinkTypes.STRING, AlinkTypes.LONG}
		);

		this.setOutputTable(new MTable(list, schema));
	}
}
