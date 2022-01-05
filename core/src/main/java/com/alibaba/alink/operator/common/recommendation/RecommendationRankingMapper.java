package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableTypes;
import com.alibaba.alink.common.mapper.MapperChain;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.recommendation.RecommendationRankingParams;
import com.alibaba.alink.pipeline.ModelExporterUtils;

import java.util.ArrayList;
import java.util.List;

public class RecommendationRankingMapper extends ModelMapper {

	private static final long serialVersionUID = -3353498411027168031L;
	private MapperChain mapperList;
	private List <Row> modelRows;
	private int itemListIdx;
	private int schemaLen;

	private String[] recallNames;
	private String scoreCol;
	private int scoreIndex;
	private int topN;
	private String mTableSchemaStr;

	public RecommendationRankingMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.modelRows = modelRows;
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		itemListIdx = TableUtil.findColIndex(dataSchema.getFieldNames(),
			params.get(RecommendationRankingParams.M_TABLE_COL));
		String outputCol = params.get(RecommendationRankingParams.OUTPUT_COL);
		String[] reservedCols = params.contains(RecommendationRankingParams.RESERVED_COLS)
			? params.get(RecommendationRankingParams.RESERVED_COLS) : getDataSchema().getFieldNames();
		scoreCol = params.contains(RecommendationRankingParams.RANKING_COL) ?
			params.get(RecommendationRankingParams.RANKING_COL) : KObjectUtil.SCORE_NAME;
		topN = params.get(RecommendationRankingParams.TOP_N);
		if(outputCol == null) {
			outputCol = params.get(RecommendationRankingParams.M_TABLE_COL);
		}
		return Tuple4.of(dataSchema.getFieldNames(), new String[] {outputCol},
			new TypeInformation <?>[] {MTableTypes.M_TABLE}, reservedCols);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		MTable recallData = (MTable) selection.get(itemListIdx);
		if (mapperList == null) {
			TableSchema mTableSchema = recallData.getTableSchema();

			String[] dataNames = getDataSchema().getFieldNames();
			this.recallNames = mTableSchema.getFieldNames();
			TypeInformation <?>[] dataTypes = getDataSchema().getFieldTypes();
			TypeInformation <?>[] recallTypes = mTableSchema.getFieldTypes();

			schemaLen = recallNames.length + dataNames.length - 1;

			String[] allNames = new String[schemaLen];
			TypeInformation <?>[] allTypes = new TypeInformation <?>[schemaLen];
			int cnt = 0;
			for (int i = 0; i < dataNames.length; ++i) {
				if (i != itemListIdx) {
					allNames[cnt] = dataNames[i];
					allTypes[cnt++] = dataTypes[i];
				} else {
					for (int j = 0; j < recallNames.length; ++j) {
						allNames[cnt] = recallNames[j];
						allTypes[cnt++] = recallTypes[j];
					}
				}
			}

			this.mapperList = ModelExporterUtils.loadMapperListFromStages(modelRows, getModelSchema(),
				new TableSchema(allNames, allTypes));

			String[] resultNames = this.mapperList.getOutTableSchema().getFieldNames();
			this.scoreIndex = scoreCol != null ? TableUtil.findColIndex(resultNames, scoreCol) :
				resultNames.length - 1;
			StringBuilder build = new StringBuilder();
			for (int j = 0; j < recallNames.length; ++j) {
				build.append(recallNames[j]).append(" ")
					.append(FlinkTypeConverter.getTypeString(recallTypes[j]));
				build.append(", ");

			}
			build.append(scoreCol).append(" DOUBLE");
			mTableSchemaStr = build.toString();
		}

		List <Row> rows = recallData.getRows();
		Object[][] itemLists = new Object[recallNames.length][rows.size()];
		String[] names = recallData.getColNames();
		int[] indices = TableUtil.findColIndices(names, recallNames);
		for (int i = 0; i < recallNames.length; ++i) {
			for (int j = 0; j < rows.size(); ++j) {
				itemLists[i][j] = rows.get(j).getField(indices[i]);
			}
		}

		int itemSize = itemLists[0].length;

		double[] scores = new double[itemSize];
		Row outputRow;
		Row allRow = new Row(schemaLen);
		for (int i = 0; i < itemSize; ++i) {
			int cnt = 0;
			for (int j = 0; j < selection.length(); ++j) {
				if (j != itemListIdx) {
					allRow.setField(cnt++, selection.get(j));
				} else {
					for (int k = 0; k < recallNames.length; ++k) {
						allRow.setField(cnt++, itemLists[k][i]);
					}
				}
			}
			outputRow = this.mapperList.map(allRow);
			scores[i] = Double.parseDouble(outputRow.getField(scoreIndex).toString());
		}
		int localTopK = Math.min(itemSize, topN);
		RecommUtils.RecommPriorityQueue priorQueue = new RecommUtils.RecommPriorityQueue(localTopK);

		for (int i = 0; i < itemSize; ++i) {
			priorQueue.addOrReplace(i, scores[i]);
		}

		Tuple2 <List <Object>, List <Double>> itemsAndScores = priorQueue.getOrderedObjects();
		List <Row> ret = new ArrayList <>(rows.size());
		for (int j = 0; j < localTopK; ++j) {
			Row row = new Row(recallNames.length + 1);
			for (int i = 0; i < recallNames.length; ++i) {
				row.setField(i, itemLists[i][(int) itemsAndScores.f0.get(j)]);
			}
			row.setField(recallNames.length, itemsAndScores.f1.get(j));
			ret.add(row);
		}

		MTable mTable = new MTable(ret, mTableSchemaStr);
		result.set(0, mTable);
	}
}
