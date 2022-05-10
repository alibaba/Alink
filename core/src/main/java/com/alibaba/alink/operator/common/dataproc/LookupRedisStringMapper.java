package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.io.redis.Redis;
import com.alibaba.alink.common.io.redis.RedisClassLoaderFactory;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.dataproc.LookupStringRedisParams;

/**
 * Mapper for String type Key to Values operation of Redis.
 */
public class LookupRedisStringMapper extends SISOMapper {
	private final RedisClassLoaderFactory factory;

	private transient Redis redis;

	public LookupRedisStringMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		factory = new RedisClassLoaderFactory(params.get(LookupStringRedisParams.PLUGIN_VERSION));
	}

	@Override
	protected TypeInformation <?> initOutputColType() {
		return AlinkTypes.STRING;
	}

	@Override
	protected Object mapColumn(Object input) throws Exception {
		return redis.get(input.toString());
	}

	@Override
	public void open() {
		super.open();

		redis = RedisClassLoaderFactory.create(factory).create(params);
	}

	@Override
	public void close() {
		super.close();

		if (redis != null) {
			redis.close();
		}
	}

}
