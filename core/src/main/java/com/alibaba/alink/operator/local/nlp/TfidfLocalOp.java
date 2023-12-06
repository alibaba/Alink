package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

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
import com.alibaba.alink.operator.local.sql.JoinLocalOp;
import com.alibaba.alink.params.nlp.TfIdfParams;

import java.util.ArrayList;

/*
 *The implementation of TFIDF based on the result of doc word count algorithm
 * we use table api to implement the algorithm
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "wordCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@ParamSelectColumnSpec(name = "countCol", allowedTypeCollections = TypeCollections.LONG_TYPES)
@ParamSelectColumnSpec(name = "docIdCol")
@NameCn("TF-IDF")
public final class TfidfLocalOp extends LocalOperator <TfidfLocalOp>
	implements TfIdfParams <TfidfLocalOp> {

	public TfidfLocalOp() {
		super(null);
	}

	public TfidfLocalOp(Params parameters) {
		super(parameters);
	}

	@Override
	public TfidfLocalOp linkFrom(LocalOperator <?>... inputs) {
		checkOpSize(1, inputs);

		String wordColName = this.getWordCol();
		String docIdColName = this.getDocIdCol();
		String countColName = this.getCountCol();

		LocalOperator <?> in = inputs[0];

		// Count doc and word count in a doc
		final LocalOperator docStat = in.groupBy(docIdColName,
			docIdColName + ",sum(" + countColName + ") as total_word_count");
		//Count totoal word count of words
		LocalOperator wordStat = in.groupBy(wordColName + "," + docIdColName,
				wordColName + "," + docIdColName + ",COUNT(" + docIdColName + ") as tmp_count")
			.groupBy(wordColName, wordColName + ",count(" + wordColName + ") as doc_cnt");

		final String tmpColNames = docIdColName + "," + wordColName + "," + countColName + "," + "total_word_count";
		final String tmpColNames1 = tmpColNames + ",doc_cnt";
		final int docIdIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), docIdColName);
		String tmpColNames2 = tmpColNames1 + ",total_doc_count,tf,idf,tfidf";
		TypeInformation[] types = in.getColTypes();

		LocalOperator join1 = new JoinLocalOp(docIdColName + " = docid1",
			"1 as id1," + tmpColNames)
			.linkFrom(in, docStat.as("docid1,total_word_count"));

		LocalOperator join2 = new JoinLocalOp(wordColName + " = " + "word1", "id1," + tmpColNames1)
			.linkFrom(join1
				, wordStat.as("word1,doc_cnt"));

		LocalOperator join3 = new JoinLocalOp()
			.setJoinPredicate("id1=id")
			.setSelectClause("id1," + tmpColNames1 + ", total_doc_count")
			.linkFrom(join2, docStat.select("1 as id,count(1) as total_doc_count"));

		ArrayList <Row> list = new ArrayList <>();
		for (Row row : join3.getOutputTable().getRows()) {
			Object docId = row.getField(1);
			String word = row.getField(2).toString();
			Long wordCount = (Long) row.getField(3);
			Long totalWordCount = (Long) row.getField(4);
			Long docCount = (Long) row.getField(5);
			Long totalDocCount = (Long) row.getField(6);
			double tf = 1.0 * wordCount / totalWordCount;
			double idf = Math.log(1.0 * totalDocCount / (docCount + 1));

			list.add(Row.of(docId, word, wordCount, totalWordCount, docCount, totalDocCount, tf, idf, tf * idf));
		}

		TableSchema schema = new TableSchema(
			tmpColNames2.split(","),
			new TypeInformation <?>[] {types[docIdIndex], Types.STRING, Types.LONG, Types.LONG, Types.LONG, Types.LONG,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE});
		setOutputTable(new MTable(list, schema));
		return this;
	}
}
