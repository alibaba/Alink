package com.alibaba.alink.operator.common.feature.address;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.feature.address.AddressUtil.Address;

/**
 * Binarize a continuous variable using a threshold. The features greater than threshold, will be binarized 1.0 and the
 * features equal to or less than threshold, will be binarized to 0.
 *
 * <p>Support Vector input and Number input.
 */
public class AddressParserMapper extends SISOMapper {

	public AddressParserMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

	}

	@Override
	protected TypeInformation <?> initOutputColType() {
		return AlinkTypes.STRING;
	}

	/**
	 * If input is a vector, in the case of dense vector, all the features are compared with threshold, and return a
	 * vector in either dense or sparse format, whichever uses less storage. If input is a sparseVector, only compare
	 * those non-zero features, and returns a sparse vector.
	 *
	 * @param input data input, support number or Vector.
	 * @return If input is a number, compare the number with threshold.
	 * @throws IllegalArgumentException input is neither number nor vector.
	 */
	@Override
	protected Object mapColumn(Object input) throws Exception {
		if (null == input) {
			return null;
		}
		String address = input.toString();
		Address ad = AddressUtil.parseAddress(address);
		if (ad.city == null && ad.prov == null && ad.district == null) {
			return null;
		} else {
			return JsonConverter.toJson(ad);
		}

	}
}
