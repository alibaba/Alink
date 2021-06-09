package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;
import com.alibaba.alink.params.recommendation.BaseRecommParams;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;
import com.alibaba.alink.params.recommendation.HasInitRecommCol;
import com.alibaba.alink.pipeline.recommendation.BaseRecommender;

import java.util.List;

/**
 * The ModelMapper for {@link BaseRecommender}.
 */
public class RecommMapper extends ModelMapper {

	private static final long serialVersionUID = -3353498411027168031L;
	/**
	 * (modelScheme, dataSchema, params, recommType) -> RecommKernel
	 */
	private final RecommKernel recommKernel;
	private final String initRecommCol;
	public RecommMapper(FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder,
						RecommType recommType,
						TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		this.recommKernel = recommKernelBuilder.apply(modelSchema, dataSchema, params, recommType);
		this.initRecommCol = params.get(HasInitRecommCol.INIT_RECOMM_COL);
		this.ioSchema = recommPrepareIoSchema(params, recommType, recommKernel);

		checkIoSchema();

		initializeSliced();
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		recommKernel.loadModel(modelRows);
	}

	@Override
	protected final Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {

		return Tuple4.of(new String[] {}, new String[] {}, new TypeInformation[] {}, new String[] {});
	}

	private Tuple4 <String[], String[], TypeInformation <?>[], String[]> recommPrepareIoSchema(
		Params params, RecommType recommType, RecommKernel recommKernel) {
		String[] selectedCols;
		String[] outputCols = new String[] {params.get(BaseRecommParams.RECOMM_COL)};

		TypeInformation <?>[] outputTypes;

		String itemCol;
		String userCol;
		switch (recommType) {
			case RATE:
				selectedCols = new String[] {params.get(BaseRateRecommParams.USER_COL),
					params.get(BaseRateRecommParams.ITEM_COL)};
				outputTypes = new TypeInformation <?>[] {Types.DOUBLE};
				break;
			case USERS_PER_ITEM:
			case SIMILAR_ITEMS:
				itemCol = params.get(BaseUsersPerItemRecommParams.ITEM_COL);
				selectedCols = initRecommCol == null ? new String[] {itemCol} : new String[] {itemCol, initRecommCol};
				outputTypes = new TypeInformation <?>[] {Types.STRING};
				break;
			case ITEMS_PER_USER:
			case SIMILAR_USERS:
				userCol = params.get(BaseItemsPerUserRecommParams.USER_COL);
				selectedCols = initRecommCol == null ? new String[] {userCol} : new String[] {userCol, initRecommCol};
				outputTypes = new TypeInformation <?>[] {Types.STRING};
				break;
			default:
				throw new RuntimeException("not support yet.");
		}
		return Tuple4.of(selectedCols, outputCols, outputTypes, params.get(BaseRecommParams.RESERVED_COLS));
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		if (recommKernel.recommType.equals(RecommType.RATE)) {
			Object[] input = new Object[] {selection.get(0), selection.get(1)};
			result.set(0, this.recommKernel.recommend(input));
		} else {
			if (initRecommCol != null) {
				Object[] input = new Object[] {selection.get(0)};
				String recommJson = KObjectUtil.MergeRecommJson(recommKernel.getObjectName(),
					(String) this.recommKernel.recommend(input), (String) selection.get(1));
				result.set(0, recommJson);

			} else {
				Object[] input = new Object[] {selection.get(0)};
				result.set(0, this.recommKernel.recommend(input));
			}
		}
	}
}
