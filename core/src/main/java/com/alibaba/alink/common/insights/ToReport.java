package com.alibaba.alink.common.insights;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ToReport {

	final List <Insight> insights;

	public ToReport(List <Insight> insights) {
		this.insights = insights;
	}

	public void withHtml(String folderPath, boolean overwrite) throws IOException {
		withHtml(folderPath, overwrite, Integer.MAX_VALUE);
	}

	public void withHtml(String folderPath, boolean overwrite, int topN) throws IOException {
		File folder = new File(folderPath);
		if (folder.exists()) {
			if (folder.list().length > 0) {
				if (overwrite) {
					FileUtils.deleteDirectory(folder);
					folder.mkdirs();
				} else {
					throw new RuntimeException("Folder(" + folderPath + ") exists and has contents.");
				}
			}
		} else {
			folder.mkdirs();
		}

		StringBuilder sbd = new StringBuilder();
		sbd.append("<html>")
			.append("<head><meta http-equiv='Content-Type' content='text/html; charset=UTF-8'></head>")
			.append("<body><table border='1'>")
			.append("<tr><td>排名</td><td>可视化</td><td>说明</td></tr>");
		int rank = 1;
		for (Insight insight : insights) {
			if (rank > topN) {
				break;
			}

			try {
				JFreeChartUtil.generateChart(
					insight,
					new File(folder, "pic_" + rank + ".jpg").getAbsolutePath()
				);
			} catch (Exception ex) {
				System.out.println(insight);
				ex.printStackTrace();
			}

			sbd.append("<tr>")
				.append("<td>")
				.append(rank)
				.append("</td>")
				.append("<td>")
				.append("<img src='pic_").append(rank).append(".jpg'>")
				.append("</td>")
				.append("<td>")
				.append("<pre>")
				.append(insight.toString())
				.append("</pre>")
				.append("</td>")
				.append("</tr>");
			rank++;
		}
		sbd.append("</table></body></html>");

		File html = new File(folder, "index.html");
		FileWriter writer = new FileWriter(html);
		writer.write(sbd.toString());
		writer.close();
	}
}
