package com.alibaba.alink.common.io.catalog.hive.plugin.initializer;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;

public final class HivePluginInitializer {

    public static synchronized void initialize(String hadoopConfigure) {
        org.apache.hadoop.conf.Configuration hadoopConfiguration = new HdfsConfiguration();

        if (!addHadoopConfIfFound(hadoopConfiguration, hadoopConfigure)) {
        	throw new IllegalStateException("Could not find the hadoopConfigure in " + hadoopConfigure);
		}

        UserGroupInformation.setConfiguration(hadoopConfiguration);
    }

	private static boolean addHadoopConfIfFound(org.apache.hadoop.conf.Configuration configuration, String possibleHadoopConfPath) {
		boolean foundHadoopConfiguration = false;
		if (new File(possibleHadoopConfPath).exists()) {
			if (new File(possibleHadoopConfPath + "/core-site.xml").exists()) {
				configuration.addResource(new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/core-site.xml"));
				foundHadoopConfiguration = true;
			}
			if (new File(possibleHadoopConfPath + "/hdfs-site.xml").exists()) {
				configuration.addResource(new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/hdfs-site.xml"));
				foundHadoopConfiguration = true;
			}
		}
		return foundHadoopConfiguration;
	}
}
