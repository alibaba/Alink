package com.alibaba.alink.operator.batch.utils;

import com.alibaba.alink.metadata.def.shaded.com.google.protobuf.TextFormat;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatisticsList;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class StatsVisualizerTest extends AlinkTestBase {
	@Test
	public void testVisualize() throws IOException {
		DatasetFeatureStatisticsList stats;
		try (InputStream statsInputStream = getClass().getResourceAsStream("/example_stats.dat")) {
			assert statsInputStream != null;
			String s = IOUtils.toString(statsInputStream);
			stats = TextFormat.parse(s, DatasetFeatureStatisticsList.class);
		}
		StatsVisualizer visualizer = StatsVisualizer.getInstance();
		visualizer.visualize(stats, null);
	}
}
