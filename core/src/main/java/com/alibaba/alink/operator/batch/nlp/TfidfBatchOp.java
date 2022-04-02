package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sql.JoinBatchOp;
import com.alibaba.alink.params.nlp.TfIdfParams;

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
public final class TfidfBatchOp extends BatchOperator <TfidfBatchOp>
	implements TfIdfParams <TfidfBatchOp> {

	private static final long serialVersionUID = -1183182290899689559L;

	public TfidfBatchOp() {
		super(null);
	}

	public TfidfBatchOp(Params parameters) {
		super(parameters);
	}

	@Override
	public TfidfBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(1, inputs);

		String wordColName = this.getWordCol();
		String docIdColName = this.getDocIdCol();
		String countColName = this.getCountCol();

		BatchOperator <?> in = inputs[0];

		// Count doc and word count in a doc
		final BatchOperator docStat = in.groupBy(docIdColName,
			docIdColName + ",sum(" + countColName + ") as total_word_count");
		//Count totoal word count of words
		BatchOperator wordStat = in.groupBy(wordColName + "," + docIdColName,
			wordColName + "," + docIdColName + ",COUNT(1 ) as tmp_count")
			.groupBy(wordColName, wordColName + ",count(1) as doc_cnt");

		final String tmpColNames = docIdColName + "," + wordColName + "," + countColName + "," + "total_word_count";
		final String tmpColNames1 = tmpColNames + ",doc_cnt";
		final int docIdIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), docIdColName);
		String tmpColNames2 = tmpColNames1 + ",total_doc_count,tf,idf,tfidf";
		TypeInformation[] types = in.getColTypes();

		BatchOperator join1 = new JoinBatchOp(docIdColName + " = docid1",
			"1 as id1," + tmpColNames)
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(in, docStat.as("docid1,total_word_count"));

		BatchOperator join2 = new JoinBatchOp(wordColName + " = " + "word1", "id1," + tmpColNames1)
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(join1
				, wordStat.as("word1,doc_cnt"));

		//Count tf idf resulst of words in docs
		this.setOutput(join2
				.getDataSet()
				.join(docStat.select("1 as id,count(1) as total_doc_count").getDataSet()
					, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
				.where("id1").equalTo("id")
				.map(new MapFunction <Tuple2 <Row, Row>, Row>() {
					private static final long serialVersionUID = 7980098907796172211L;

					@Override
					public Row map(Tuple2 <Row, Row> rowRowTuple2) throws Exception {
						Row row = new Row(9);
						Object docId = rowRowTuple2.f0.getField(1);
						String word = rowRowTuple2.f0.getField(2).toString();
						Long wordCount = (Long) rowRowTuple2.f0.getField(3);
						Long totalWordCount = (Long) rowRowTuple2.f0.getField(4);
						Long docCount = (Long) rowRowTuple2.f0.getField(5);
						Long totalDocCount = (Long) rowRowTuple2.f1.getField(1);
						double tf = 1.0 * wordCount / totalWordCount;
						double idf = Math.log(1.0 * totalDocCount / (docCount + 1));
						row.setField(0, docId);
						row.setField(1, word);
						row.setField(2, wordCount);
						row.setField(3, totalWordCount);
						row.setField(4, docCount);
						row.setField(5, totalDocCount);
						row.setField(6, tf);
						row.setField(7, idf);
						row.setField(8, tf * idf);
						return row;
					}
				}), tmpColNames2.split(","),
			new TypeInformation <?>[] {types[docIdIndex], Types.STRING, Types.LONG, Types.LONG, Types.LONG, Types.LONG,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE});
		;
		return this;
	}
}
