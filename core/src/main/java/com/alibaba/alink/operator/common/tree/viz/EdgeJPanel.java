package com.alibaba.alink.operator.common.tree.viz;

import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;

import javax.swing.*;
import java.awt.*;

public class EdgeJPanel extends JPanel {

	private TreeModelDataConverter model;
	private MultiStringIndexerModelData stringIndexerModelData;
	private TreeModelViz.Node4CalcPos node4CalcPos;
	private TreeModelViz.Node4CalcPos[] all;
	private Color lineColor = Color.RED;
	NodeDimension nd;

	public EdgeJPanel(
		TreeModelDataConverter model,
		TreeModelViz.Node4CalcPos node4CalcPos,
		TreeModelViz.Node4CalcPos[] all,
		NodeDimension nd) {

		this.model = model;
		if (this.model.stringIndexerModelSerialized != null) {
			this.stringIndexerModelData =
				new MultiStringIndexerModelDataConverter()
					.load(this.model.stringIndexerModelSerialized);
		}
		this.node4CalcPos = node4CalcPos;
		this.all = all;
		this.nd = nd;
		initComponents();
	}

	@Override
	public void setFont(Font font) {
		super.setFont(font);
		this.setUserComponents(font);
	}

	private void setUserComponents(Font font) {
		Component[] cs = this.getComponents();
		for (int i = 0; i < cs.length; i++) {
			cs[i].setFont(font);
		}
	}

	@Override
	public void reshape(int leftX, int leftY, int width, int height) {
		super.reshape(leftX, leftY, width, height);
		drawEdgeJPanel(leftX, leftY, width, height);
	}

	private void drawEdgeJPanel(int leftX, int leftY, int w, int h) {
		if (node4CalcPos == null || node4CalcPos.node.isLeaf()) {
			throw new IllegalArgumentException("node has no child to draw edge!");
		}

		this.drawEdgeJPanelWithWidth(leftX, leftY, w, h);
	}

	private void drawThreeLabel(int x, int y, int w, int h, int linewidth, String showstr) {
		double steph = h / 2.0;
		double substep = (1.0 / 4.0 * steph);
		JLabel l = new JLabel("");
		l.setOpaque(true);
		l.setBackground(lineColor);
		l.setBounds(x, y, linewidth, (int) substep);

		JLabel l2 = new JLabel(showstr);
		l2.setHorizontalAlignment(SwingConstants.CENTER);
		l2.setToolTipText(showstr);
		l2.setBounds(x - this.nd.nodeWidth / 2, y + (int) substep, this.nd.nodeWidth, (int) (2 * substep));


		JLabel l3 = new JLabel("");
		l3.setOpaque(true);
		l3.setBackground(lineColor);
		l3.setBounds(x, y + (int) (3 * substep), linewidth, (int) substep);

		this.add(l);
		this.add(l2);
		this.add(l3);
	}

	private void drawOneVericalLine(int x, int y, int w, int h, int linewidth, String showstr) {
		this.drawThreeLabel(x, y, w, h, linewidth, showstr);
	}

	private void drawEdgeJPanelWithWidth(int leftX, int leftY, int w, int h) {
		int steph = h / 2;
		int x = node4CalcPos.gNode.posCenter - leftX;
		int y = 0;
		//third vertical
		float levelMaxLineWidth = this.drawThirdVerticalLineWithWidth(leftX, leftY, w, h, steph);
		int firstlinewidth = levelMaxLineWidth < 1.0 ? 1 : (int) levelMaxLineWidth;
		//first vertical
		this.drawOneVericalLine(x, y, w, h, firstlinewidth,
			String.valueOf(model.meta.get(HasFeatureCols.FEATURE_COLS)[node4CalcPos.node.getFeatureIndex()])
		);
		//secord horizon
		this.drawSecondHorizonLineWithWidth(leftX, leftY, w, h, x, steph);
	}

	private float drawThirdVerticalLineWithWidth(int leftX, int leftY, int w, int h, int steph) {
		float levelMaxLineWidth = 0;

		for (int i = 0; i < node4CalcPos.childrenIdx.length; i++) {
			TreeModelViz.Node4CalcPos tmp = all[node4CalcPos.childrenIdx[i]];
			int x2 = tmp.gNode.posCenter - leftX;
			int y2 = tmp.gNode.posTop - leftY;
			int x1 = x2;
			int y1 = steph;

			double linepercent = all[node4CalcPos.childrenIdx[i]].node.getCounter().getWeightSum()
				/ node4CalcPos.node.getCounter().getWeightSum();
			if (linepercent > 1.0) {
				linepercent = 0.0;
			}
			//if node is not root , multiply node
			if (node4CalcPos.parentIdx >= 0) {
				linepercent *= node4CalcPos.gNode.linepercent;
			}
			tmp.gNode.linepercent = linepercent;

			float linewidth = (float) linepercent * this.nd.getEdgeMaxWidth();

			if (levelMaxLineWidth < linewidth) {
				levelMaxLineWidth = linewidth;
			}

			int secordlinewidth = linewidth < 1.0 ? 1 : (int) linewidth;

			String showLabel;
			if (node4CalcPos.node.getCategoricalSplit() == null) {
				if (i == 0) {
					showLabel = String.format("%s %s", "<=", String.valueOf(node4CalcPos.node.getContinuousSplit()));
				} else {
					showLabel = String.format("%s %s", ">", String.valueOf(node4CalcPos.node.getContinuousSplit()));
					;
				}
			} else {
				StringBuilder sbd = new StringBuilder();

				int inSize = 0;
				for (int j = 0; j < node4CalcPos.node.getCategoricalSplit().length; ++j) {
					if (node4CalcPos.node.getCategoricalSplit()[j] == i) {
						if (inSize > 0) {
							sbd.append(",");
						}
						sbd.append(
							stringIndexerModelData.getToken(
								model.meta.get(HasFeatureCols.FEATURE_COLS)[node4CalcPos.node.getFeatureIndex()],
								Long.valueOf(j))
						);
						inSize++;
					}
				}

				showLabel = String.format("%s %s", "IN", sbd.toString());
			}

			this.drawOneVericalLine(x1, y1, w, h, secordlinewidth, showLabel);

		}
		return levelMaxLineWidth;
	}

	private void drawSecondHorizonLineWithWidth(int leftX, int leftY, int w, int h, int x, int steph) {
		//left
		float leftMaxLineWidth = 0;
		for (int i = 0; i < node4CalcPos.childrenIdx.length; i++) {
			TreeModelViz.Node4CalcPos tmp = all[node4CalcPos.childrenIdx[i]];
			int x2 = tmp.gNode.posCenter - leftX;
			if (x2 >= x) {
				break;
			}
			int x3 = 0;

			if ((i + 1) > (node4CalcPos.childrenIdx.length - 1)) {
				x3 = x;
			} else {
				x3 = all[node4CalcPos.childrenIdx[i + 1]].gNode.posCenter - leftX;
				if (x3 > x) {
					x3 = x;
				}
			}

			float linewidth = (float) tmp.gNode.linepercent * this.nd.getEdgeMaxWidth();
			if (leftMaxLineWidth < linewidth) {
				leftMaxLineWidth = linewidth;
			}

			int thirdlinewidth = leftMaxLineWidth < 1.0 ? 1 : (int) leftMaxLineWidth;

			JLabel l4 = new JLabel("");
			l4.setOpaque(true);
			l4.setBackground(lineColor);
			l4.setBounds(x2, steph, x3 - x2, thirdlinewidth);

			this.add(l4);
		}

		//right
		float rightMaxLineWidth = 0;
		for (int i = node4CalcPos.childrenIdx.length - 1; i >= 0; i--) {
			TreeModelViz.Node4CalcPos tmp = all[node4CalcPos.childrenIdx[i]];
			int x2 = tmp.gNode.posCenter - leftX;
			if (x2 <= x) {
				break;
			}

			int x3 = 0;
			if ((i - 1) < 0) {
				x3 = x;
			} else {
				x3 = all[node4CalcPos.childrenIdx[i - 1]].gNode.posCenter - leftX;
				if (x3 < x) {
					x3 = x;
				}
			}

			float linewidth = (float) tmp.gNode.linepercent * this.nd.getEdgeMaxWidth();
			if (rightMaxLineWidth < linewidth) {
				rightMaxLineWidth = linewidth;
			}
			int thirdlinewidth = rightMaxLineWidth < 1.0 ? 1 : (int) rightMaxLineWidth;

			JLabel l4 = new JLabel("");
			l4.setOpaque(true);
			l4.setBackground(lineColor);
			l4.setBounds(x3, steph, x2 - x3, thirdlinewidth);


			this.add(l4);
		}
	}

	private void initComponents() {

		GroupLayout layout = new GroupLayout(this);
		this.setLayout(layout);
		layout.setHorizontalGroup(
			layout.createParallelGroup(GroupLayout.Alignment.LEADING)
				.addGap(0, 400, Short.MAX_VALUE)
		);
		layout.setVerticalGroup(
			layout.createParallelGroup(GroupLayout.Alignment.LEADING)
				.addGap(0, 300, Short.MAX_VALUE)
		);
	}
}
