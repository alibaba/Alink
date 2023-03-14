package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.params.dl.HasPythonEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ProphetForDesignerMapper extends ProphetMapper {

	final static String PYTHON_ENV_TAR_NAME = "./prophet_env.tar.gz/tf115-ai030-py36-linux";
	private static final Logger LOG = LoggerFactory.getLogger(ProphetForDesignerMapper.class);

	public ProphetForDesignerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	public void open() {
		File file = new File(PYTHON_ENV_TAR_NAME);
		if (file.exists()) {
			LOG.info("PYTHON_ENV_TAR_NAME is exist.");
			LOG.info("PYTHON_ENV_TAR_NAME AbsolutePath:" + file.getAbsolutePath());
			params.set(HasPythonEnv.PYTHON_ENV, file.getAbsolutePath());
		}

		super.open();
	}

}


