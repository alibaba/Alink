package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.MTableUtil.GroupFunction;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.common.nlp.Method;
import com.alibaba.alink.operator.common.nlp.TextRank;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.dataproc.AppendIdLocalOp;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import com.alibaba.alink.operator.local.sql.JoinLocalOp;
import com.alibaba.alink.params.nlp.KeywordsExtractionParams;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Automatically identify in a text a set of terms that best describe the document based on TextRank.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("关键词抽取")
public final class KeywordsExtractionLocalOp extends LocalOperator <KeywordsExtractionLocalOp>
	implements KeywordsExtractionParams <KeywordsExtractionLocalOp> {

	/**
	 * default constructor.
	 */
	public KeywordsExtractionLocalOp() {
		super(null);
	}

	public KeywordsExtractionLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
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

		LocalOperator <?> inWithId = in
			.link(
				new AppendIdLocalOp()
					.setIdCol(AppendIdLocalOp.appendIdColName)
			);

		LocalOperator <?> filterd = inWithId
			.link(
				new StopWordsRemoverLocalOp()
					.setSelectedCol(selectedColName)
					.setOutputCol("selectedColName")
			);

		LocalOperator <?> weights;

		switch (method) {
			case TF_IDF: {
				weights = filterd
					.link(
						new DocWordCountLocalOp()
							.setDocIdCol(AppendIdBatchOp.appendIdColName)
							.setContentCol("selectedColName")
					)
					.link(
						new TfidfLocalOp()
							.setDocIdCol(AppendIdBatchOp.appendIdColName)
							.setWordCol("word")
							.setCountCol("cnt")
					)
					.select(new String[] {AppendIdBatchOp.appendIdColName, "word", "tfidf"});

				break;
			}
			case TEXT_RANK: {
				final Params params = getParams();
				RowCollector rowCollector = new RowCollector();
				for (Row row : filterd.select(new String[] {AppendIdBatchOp.appendIdColName, "selectedColName"})
					.getOutputTable().getRows()) {
					Row[] out = TextRank.getKeyWords(row,
						params.get(KeywordsExtractionParams.DAMPING_FACTOR),
						params.get(KeywordsExtractionParams.WINDOW_SIZE),
						params.get(KeywordsExtractionParams.MAX_ITER),
						params.get(KeywordsExtractionParams.EPSILON));
					for (int i = 0; i < out.length; i++) {
						rowCollector.collect(out[i]);
					}
				}

				weights = new TableSourceLocalOp(new MTable(rowCollector.getRows(),
					AppendIdBatchOp.appendIdColName + " long, " + "word string, tfidf double"));

				break;
			}
			default: {
				throw new AkUnsupportedOperationException("Not support extraction type: " + method);
			}
		}

		List <Row> res = MTableUtil.groupFunc(weights.getOutputTable(), new String[] {AppendIdBatchOp.appendIdColName},
			new GroupFunction() {
				@Override
				public void calc(List <Row> list, Collector <Row> collector) {
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
		TableSourceLocalOp tmpTable = new TableSourceLocalOp(new MTable(res,
			new TableSchema(new String[] {docId, outputColName}, new TypeInformation[] {Types.LONG, Types.STRING})));

		StringBuilder selectClause = new StringBuilder("a." + outputColName);
		String[] keepColNames = outputColsHelper.getReservedColumns();
		for (int i = 0; i < keepColNames.length; i++) {
			selectClause.append("," + keepColNames[i]);
		}
		JoinLocalOp join = new JoinLocalOp()
			.setType("join")
			.setSelectClause(selectClause.toString())
			.setJoinPredicate(docId + "=" + AppendIdBatchOp.appendIdColName)
			.linkFrom(tmpTable, inWithId);

		this.setOutputTable(join.getOutputTable());
	}

}
