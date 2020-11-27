package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.JoinBatchOp;
import com.alibaba.alink.operator.common.nlp.Method;
import com.alibaba.alink.operator.common.nlp.TextRank;
import com.alibaba.alink.params.nlp.KeywordsExtractionParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Automatically identify in a text a set of terms that best describe the document based on TextRank.
 */
public final class KeywordsExtractionBatchOp extends BatchOperator <KeywordsExtractionBatchOp>
	implements KeywordsExtractionParams <KeywordsExtractionBatchOp> {

	private static final long serialVersionUID = 3780919803958920490L;

	/**
	 * default constructor.
	 */
	public KeywordsExtractionBatchOp() {
		super(null);
	}

	public KeywordsExtractionBatchOp(Params params) {
		super(params);
	}

	@Override
	public KeywordsExtractionBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final String docId = "doc_alink_id";
		String selectedColName = this.getSelectedCol();
		TableUtil.assertSelectedColExist(in.getColNames(), selectedColName);
		String outputColName = this.getOutputCol();
		if (null == outputColName) {
			outputColName = selectedColName;
		}
		OutputColsHelper outputColsHelper = new OutputColsHelper(in.getSchema(), outputColName, Types.STRING);

		final Integer topN = this.getTopN();
		Method method = this.getMethod();

		BatchOperator inWithId = new TableSourceBatchOp(
			AppendIdBatchOp.appendId(in.getDataSet(), in.getSchema(), getMLEnvironmentId()))
			.setMLEnvironmentId(getMLEnvironmentId());

		DataSet <Row> weights;
		StopWordsRemoverBatchOp filterOp = new StopWordsRemoverBatchOp()
			.setMLEnvironmentId(getMLEnvironmentId())
			.setSelectedCol(selectedColName)
			.setOutputCol("selectedColName");

		BatchOperator filtered = filterOp.linkFrom(inWithId);

		switch (method) {
			case TF_IDF: {
				DocWordCountBatchOp wordCount = new DocWordCountBatchOp()
					.setMLEnvironmentId(getMLEnvironmentId())
					.setDocIdCol(AppendIdBatchOp.appendIdColName)
					.setContentCol("selectedColName");
				TfidfBatchOp tfIdf = new TfidfBatchOp()
					.setMLEnvironmentId(getMLEnvironmentId())
					.setDocIdCol(AppendIdBatchOp.appendIdColName)
					.setWordCol("word")
					.setCountCol("cnt");

				BatchOperator op = filtered.link(wordCount).link(tfIdf);
				weights = op.select(AppendIdBatchOp.appendIdColName + ", " + "word, tfidf").getDataSet();
				break;
			}
			case TEXT_RANK: {
				DataSet <Row> data = filtered.select(AppendIdBatchOp.appendIdColName + ", selectedColName")
					.getDataSet();
				// Initialize the TextRank class, which runs the text rank algorithm.
				final Params params = getParams();
				weights = data.flatMap(new FlatMapFunction <Row, Row>() {
					private static final long serialVersionUID = -4083643981693873537L;

					@Override
					public void flatMap(Row row, Collector <Row> collector) throws Exception {
						// For each row, apply the text rank algorithm to get the key words.
						Row[] out = TextRank.getKeyWords(row,
							params.get(KeywordsExtractionParams.DAMPING_FACTOR),
							params.get(KeywordsExtractionParams.WINDOW_SIZE),
							params.get(KeywordsExtractionParams.MAX_ITER),
							params.get(KeywordsExtractionParams.EPSILON));
						for (int i = 0; i < out.length; i++) {
							collector.collect(out[i]);
						}
					}
				});
				break;
			}
			default: {
				throw new RuntimeException("Not support this type!");
			}
		}

		DataSet <Row> res = weights.groupBy(new KeySelector <Row, String>() {
			private static final long serialVersionUID = 801794449492798203L;

			@Override
			public String getKey(Row row) {
				Object obj = row.getField(0);
				if (obj == null) {
					return "NULL";
				}
				return row.getField(0).toString();
			}
		})
			.reduceGroup(new GroupReduceFunction <Row, Row>() {
				private static final long serialVersionUID = -4051509261188494119L;

				@Override
				public void reduce(Iterable <Row> rows, Collector <Row> collector) {
					List <Row> list = new ArrayList <>();
					for (Row row : rows) {
						list.add(row);
					}
					Collections.sort(list, new Comparator <Row>() {
						@Override
						public int compare(Row row1, Row row2) {
							Double v1 = (double) row1.getField(2);
							Double v2 = (double) row2.getField(2);
							return v2.compareTo(v1);
						}
					});
					int len = Math.min(list.size(), topN);
					Row out = new Row(2);
					StringBuilder builder = new StringBuilder();
					for (int i = 0; i < len; i++) {
						builder.append(list.get(i).getField(1).toString());
						if (i != len - 1) {
							builder.append(" ");
						}
					}
					out.setField(0, list.get(0).getField(0));
					out.setField(1, builder.toString());
					collector.collect(out);
				}
			});

		// Set the output into table.
		Table tmpTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), res, new String[] {docId, outputColName},
			new TypeInformation[] {Types.LONG, Types.STRING});

		StringBuilder selectClause = new StringBuilder("a." + outputColName);
		String[] keepColNames = outputColsHelper.getReservedColumns();
		for (int i = 0; i < keepColNames.length; i++) {
			selectClause.append("," + keepColNames[i]);
		}
		JoinBatchOp join = new JoinBatchOp()
			.setMLEnvironmentId(getMLEnvironmentId())
			.setType("join")
			.setSelectClause(selectClause.toString())
			.setJoinPredicate(docId + "=" + AppendIdBatchOp.appendIdColName);

		this.setOutputTable(join.linkFrom(new TableSourceBatchOp(tmpTable).setMLEnvironmentId(getMLEnvironmentId()),
			inWithId).getOutputTable());

		return this;
	}

}
