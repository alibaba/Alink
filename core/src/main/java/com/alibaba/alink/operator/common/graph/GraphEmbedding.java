package com.alibaba.alink.operator.common.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionAllBatchOp;
import com.alibaba.alink.operator.common.nlp.WordCountUtil;
import com.alibaba.alink.params.nlp.walk.HasSourceCol;
import com.alibaba.alink.params.nlp.walk.HasTargetCol;
import com.alibaba.alink.params.nlp.walk.HasTypeCol;
import com.alibaba.alink.params.nlp.walk.HasVertexCol;
import com.alibaba.alink.params.nlp.walk.HasWeightCol;

public class GraphEmbedding {
	public static final String SOURCE_COL = "sourcecol";
	public static final String TARGET_COL = "targetcol";
	public static final String WEIGHT_COL = "weightcol";
	public static final String NODE_COL = "node";
	public static final String NODE_INDEX_COL = "nodeidxcol";
	public static final String NODE_TYPE_COL = "nodetypecol";
	private static final String TEMP_NODE_COL = "tempnodecol";

	/**
	 * Transform vertex with index
	 * vocab, schema{NODE_COL:originalType, NODE_INDEX_COL:long}
	 * indexedGraph,schema {SOURCE_COL:long, TARGET_COL:long, WEIGHT_COL:double}
	 * indexWithType, if in2 is not null, then returned, schema {NODE_INDEX_COL:long, NODE_TYPE_COL:string}
	 *
	 * @param in1    is graph data
	 * @param in2    is the vertexList with vertexType, optional
	 * @param params user inputted parameters
	 * @return
	 */
	public static BatchOperator[] trans2Index(BatchOperator in1, BatchOperator in2, Params params) {
		String sourceColName = params.get(HasSourceCol.SOURCE_COL);
		String targetColName = params.get(HasTargetCol.TARGET_COL);
		String clause;
		if (params.contains(HasWeightCol.WEIGHT_COL)) {
			String weightColName = params.get(HasWeightCol.WEIGHT_COL);
			clause = "`" + sourceColName + "`, `" + targetColName + "`, `" + weightColName + "`";
		} else {
			clause = "`" + sourceColName + "`, `" + targetColName + "`, 1.0";
		}

		BatchOperator in = in1.select(clause).as(SOURCE_COL + ", " + TARGET_COL + ", " + WEIGHT_COL);

		//count the times that all the words appear in the edges.
		BatchOperator wordCnt = WordCountUtil.count(
			new UnionAllBatchOp()
				.setMLEnvironmentId(in1.getMLEnvironmentId())
				.linkFrom(in.select(SOURCE_COL), in.select(TARGET_COL))
				.as(NODE_COL),
			NODE_COL
		);

		//name each vocab with its index.
		BatchOperator vocab = WordCountUtil.randomIndexVocab(wordCnt, 0)
			.select(WordCountUtil.WORD_COL_NAME + " AS " + NODE_COL + ", " + WordCountUtil.INDEX_COL_NAME
				+ " AS " + NODE_INDEX_COL);
		//transform input and vocab to dataSet<Tuple>
		DataSet <Tuple> inDataSet = in.getDataSet()
			.map(new MapFunction <Row, Tuple3 <Comparable, Comparable, Comparable>>() {
				private static final long serialVersionUID = 8473819294214049730L;

				@Override
				public Tuple3 <Comparable, Comparable, Comparable> map(Row value) throws Exception {
					return Tuple3.of((Comparable) value.getField(0), (Comparable) value.getField(1),
						(Comparable) value.getField(2));
				}
			});
		DataSet <Tuple2> vocabDataSet = vocab.getDataSet().map(new MapFunction <Row, Tuple2 <Comparable, Long>>() {
			private static final long serialVersionUID = 7241884458236714150L;

			@Override
			public Tuple2 <Comparable, Long> map(Row value) throws Exception {
				return Tuple2.of((Comparable) value.getField(0), (Long) value.getField(1));
			}
		});

		//join operation
		DataSet <Tuple> joinWithSourceColTuple = HackBatchOpJoin.join(
			inDataSet, vocabDataSet,
			0, 0,
			new int[][] {{1, 1}, {0, 1}, {0, 2}});
		DataSet <Tuple> indexGraphTuple = HackBatchOpJoin.join(
			joinWithSourceColTuple, vocabDataSet,
			1, 0,
			new int[][] {{0, 0}, {1, 1}, {0, 2}});
		//build batchOperator
		TypeInformation <?>[] inTypes = in.getColTypes();
		TypeInformation <?>[] vocabTypes = vocab.getColTypes();
		BatchOperator indexedGraphBatchOp = new TableSourceBatchOp(DataSetConversionUtil.toTable(
			in.getMLEnvironmentId(),
			indexGraphTuple.map(new MapFunction <Tuple, Row>() {
				private static final long serialVersionUID = -5386264086074581748L;

				@Override
				public Row map(Tuple value) throws Exception {
					Row res = new Row(3);
					res.setField(0, value.getField(0));
					res.setField(1, value.getField(1));
					res.setField(2, value.getField(2));
					return res;
				}
			}),
			new String[] {SOURCE_COL, TARGET_COL, WEIGHT_COL},
			new TypeInformation <?>[] {vocabTypes[1], vocabTypes[1], inTypes[2]}));

		if (null == in2) {

			return new BatchOperator[] {vocab, indexedGraphBatchOp};

		} else {
			BatchOperator in2Selected = in2
				.select("`" + params.get(HasVertexCol.VERTEX_COL) + "`, `" + params
					.get(HasTypeCol.TYPE_COL) + "`")
				.as(TEMP_NODE_COL + ", " + NODE_TYPE_COL);
			TypeInformation <?>[] types = new TypeInformation[2];

			types[1] = in2.getColTypes()[TableUtil.findColIndex(in2.getSchema(), params.get(HasTypeCol.TYPE_COL))];
			types[0] = vocab.getColTypes()[TableUtil.findColIndex(vocab.getSchema(), NODE_INDEX_COL)];

			DataSet <Tuple> in2Tuple = in2Selected
				.getDataSet().map(new MapFunction <Row, Tuple2 <Comparable, Comparable>>() {
					private static final long serialVersionUID = 3459700988499538679L;

					@Override
					public Tuple2 <Comparable, Comparable> map(Row value) throws Exception {
						Tuple2 <Comparable, Comparable> res = new Tuple2 <>();
						res.setField(value.getField(0), 0);
						res.setField(value.getField(1), 1);
						return res;
					}
				});
			DataSet <Row> indexWithTypeRow = HackBatchOpJoin.join(in2Tuple, vocabDataSet,
				0, 0,
				new int[][] {{1, 1}, {0, 1}})
				.map(new MapFunction <Tuple, Row>() {
					private static final long serialVersionUID = -5747375637774394150L;

					@Override
					public Row map(Tuple value) throws Exception {
						int length = value.getArity();
						Row res = new Row(length);
						for (int i = 0; i < length; i++) {
							res.setField(i, value.getField(i));
						}
						return res;
					}
				});

			BatchOperator indexWithType = new TableSourceBatchOp(
				DataSetConversionUtil.toTable(
					in.getMLEnvironmentId(),
					indexWithTypeRow,
					new String[] {NODE_INDEX_COL, NODE_TYPE_COL},
					types
				))
				.setMLEnvironmentId(in.getMLEnvironmentId());
			return new BatchOperator[] {vocab, indexedGraphBatchOp, indexWithType};

		}

	}

	/**
	 * Use join on DataSet to use strategy.
	 */
	private static class HackBatchOpJoin {
		static DataSet <Tuple> join(
			DataSet <Tuple> left,
			DataSet <Tuple2> right,
			int leftCols,
			int rightCols,
			int[][] projectOrder) {
			JoinOperator.DefaultJoin <Tuple, Tuple2> join = left
				.join(right, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
				.where(leftCols)
				.equalTo(rightCols);
			JoinOperator.ProjectJoin <Tuple, Tuple2, Tuple> projectJoin;
			int[] first = projectOrder[0];
			if (first[0] == 0) {
				projectJoin = join.projectFirst(first[1]);
			} else {
				projectJoin = join.projectSecond(first[1]);
			}
			for (int i = 1; i < projectOrder.length; i++) {
				if (projectOrder[i][0] == 0) {
					projectJoin = projectJoin.projectFirst(projectOrder[i][1]);
				} else {
					projectJoin = projectJoin.projectSecond(projectOrder[i][1]);
				}
			}
			return projectJoin;
		}
	}
}


