/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * NodeJPanel.java
 *
 * Created on 2013-1-15, 16:27:20
 */
package com.alibaba.alink.operator.common.tree.viz;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;
import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableColumnModel;

import java.awt.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NodeJPanel extends javax.swing.JPanel {

	private static final long serialVersionUID = -8394005027707336689L;
	private Color panelColor = Color.WHITE;
	private Color sourceColor = Color.WHITE;
	private Color destColor = new Color(102, 204, 255);
	private int panelBorderWidth = 2;
	TreeModelDataConverter model;
	TreeModelViz.Node4CalcPos node4CalcPos;
	List <List <Object>> list;
	double maxPercent = 0.0;

	public NodeJPanel(TreeModelDataConverter model, TreeModelViz.Node4CalcPos root,
					  Tuple2 <Double, Double> minMaxPercent) {
		this.model = model;
		this.node4CalcPos = root;
		init(minMaxPercent);
	}

	public static String printOneDecimalWithSuffix(double val) {
		return val == Math.floor(val) && !Double.isInfinite(val) ?
			String.format("%d%s", (int) val, "%") : String.format("%.1f%s", val, "%");
	}

	public static String print(double val) {
		//		return val == Math.floor(val) && !Double.isInfinite(val) ?
		//			String.format("%d", (int) val) : String.format("%f", val);
		return String.valueOf(val);
	}

	public static double multiNodeDistribution(TreeModelViz.Node4CalcPos node4CalcPos) {
		double multi = 1.0;

		Node node = node4CalcPos.node;
		if (node.getCounter().getDistributions() == null
			|| node.getCounter().getDistributions().length < 1) {
			return 0.0;
		}

		if (node.isLeaf()) {
			for (int j = 0; j < node.getCounter().getDistributions().length; ++j) {
				multi *= node.getCounter().getDistributions()[j];
			}
		} else {
			if (node.getCounter().getWeightSum() == 0.0) {
				return 0.0;
			}

			for (int j = 0; j < node.getCounter().getDistributions().length; ++j) {
				multi *= node.getCounter().getDistributions()[j] / node.getCounter().getWeightSum();
			}
		}

		return multi;
	}

	private List <List <Object>> genInfo() {
		ArrayList <List <Object>> list = new ArrayList <>();

		list.add(Arrays.asList(new Object[] {
			"Instances",
			String.valueOf(node4CalcPos.node.getCounter().getNumInst()),
			""
		}));

		list.add(Arrays.asList(new Object[] {
			"Weights",
			print(node4CalcPos.node.getCounter().getWeightSum()),
			""
		}));

		if (model.labels != null) {
			if (!node4CalcPos.node.isLeaf()) {
				for (int i = 0; i < model.labels.length; ++i) {
					double distribution = node4CalcPos.node.getCounter().getDistributions()[i];
					double weight = node4CalcPos.node.getCounter().getWeightSum();
					list.add(Arrays.asList(new Object[] {
						String.valueOf(model.labels[i]),
						print(distribution),
						printOneDecimalWithSuffix(distribution / weight * 100)
					}));
				}
			} else {
				for (int i = 0; i < model.labels.length; ++i) {
					double distribution = node4CalcPos.node.getCounter().getDistributions()[i];
					double weight = node4CalcPos.node.getCounter().getWeightSum();
					list.add(Arrays.asList(new Object[] {
						String.valueOf(model.labels[i]),
						print(distribution * weight),
						printOneDecimalWithSuffix(distribution * 100)
					}));
				}
			}
		} else {
			if (!node4CalcPos.node.isLeaf()) {
				list.add(Arrays.asList(new Object[] {
					"Sum",
					print(node4CalcPos.node.getCounter().getDistributions()[0])
				}));
			} else {
				list.add(Arrays.asList(new Object[] {
					"NormalizedSum",
					print(node4CalcPos.node.getCounter().getDistributions()[0])
				}));
			}
		}

		return list;
	}

	private void setMaxPercent(Tuple2 <Double, Double> minMaxPercent) {
		if (model.labels != null) {
			maxPercent = (multiNodeDistribution(node4CalcPos) - minMaxPercent.f0)
				/ (minMaxPercent.f1 - minMaxPercent.f0);
		} else {
			maxPercent = 1.0;
		}
	}

	private void init(Tuple2 <Double, Double> minMaxPercent) {
		initComponents();
		list = genInfo();
		setMaxPercent(minMaxPercent);

		initUserComponents();
	}

	private void initUserComponents() {
		//node id first label

		panelColor = this.getGradientColor(maxPercent);
		this.setBackground(panelColor);

		int level = node4CalcPos.level;
		final String title = "Level:" + level;
		JLabel l = new JLabel(title);
		l.setToolTipText(title);
		l.setHorizontalAlignment(SwingConstants.CENTER);
		l.setOpaque(false);
		this.add(l);

		JTable jTable1 = new JTable();
		jTable1.setShowGrid(false);
		jTable1.setOpaque(false);

		int colmunSize = list.get(0).size();
		Object[] objh = new Object[colmunSize];
		for (int i = 0; i < objh.length; i++) {
			objh[i] = "";
		}
		Object[][] objs = new Object[list.size()][colmunSize];
		for (int i = 0; i < list.size(); i++) {
			objs[i] = list.get(i).toArray();
		}

		jTable1.setModel(new javax.swing.table.DefaultTableModel(objs, objh) {

			private static final long serialVersionUID = 2938204994714516699L;
			boolean[] canEdit = new boolean[] {
				false, false, false, false, false
			};

			public boolean isCellEditable(int rowIndex, int columnIndex) {
				return canEdit[columnIndex];
			}
		});

		DefaultTableCellRenderer renderer = new DefaultTableCellRenderer();
		renderer.setBackground(panelColor);
		renderer.setHorizontalAlignment(SwingConstants.RIGHT);

		DefaultTableCellRenderer renderer1 = new DefaultTableCellRenderer();
		renderer1.setBackground(panelColor);

		jTable1.getColumnModel().getColumn(0).setCellRenderer(renderer1);
		for (int i = 1; i < colmunSize; i++) {
			jTable1.getColumnModel().getColumn(i).setCellRenderer(renderer);
		}

		for (int i = 3; i < colmunSize; i++) {
			jTable1.getTableHeader().getColumnModel().getColumn(i).setMinWidth(0);
			jTable1.getTableHeader().getColumnModel().getColumn(i).setMaxWidth(0);
			jTable1.getColumnModel().getColumn(i).setMaxWidth(0);
			jTable1.getColumnModel().getColumn(i).setMinWidth(0);
		}

		//add title
		this.add(jTable1);
	}

	private Color getGradientColor(double percent) {
		Color c;

		int r = this.destColor.getRed() - this.sourceColor.getRed();
		int g = this.destColor.getGreen() - this.sourceColor.getGreen();
		int b = this.destColor.getBlue() - this.sourceColor.getBlue();

		int nr = this.sourceColor.getRed() + ((int) (r * percent));
		int ng = this.sourceColor.getGreen() + ((int) (g * percent));
		int nb = this.sourceColor.getBlue() + ((int) (b * percent));
		c = new Color(nr, ng, nb);
		return c;
	}

	@Override
	public void reshape(int x, int y, int w, int h) {
		super.reshape(x, y, w, h);
		this.reshapeuserComponents(new Dimension(w, h));
	}

	private void reshapeuserComponents(Dimension t) {
		Component[] cs = this.getComponents();
		int size = cs.length;
		if (size < 2) {
			return;
		}

		int w = t.width - 2 * panelBorderWidth;
		int h = t.height - 2 * panelBorderWidth;
		int x = panelBorderWidth;
		int y = panelBorderWidth;

		int labelh = 15;
		if (h < labelh) {
			labelh = h;
		}
		cs[0].setBounds(x, y, w, labelh);
		y += labelh;
		cs[1].setBounds(x, y, w, h - y);

		TableColumnModel columnModel = ((JTable) cs[1]).getColumnModel();

		int colCount = columnModel.getColumnCount();

		int reservedWidth = 0;
		for (int i = 0; i < colCount - 1; ++i) {
			int width = (int) (w * 0.4);
			columnModel.getColumn(i).setMaxWidth(width);
			reservedWidth += width;
		}

		columnModel.getColumn(colCount - 1).setMaxWidth(w - reservedWidth);
	}

	@Override
	public void setFont(Font font) {
		super.setFont(font);
		this.setuserComponents(font);
	}

	private void setuserComponents(Font font) {
		Component[] cs = this.getComponents();
		for (int i = 0; i < cs.length; i++) {
			cs[i].setFont(font);
		}
	}

	private void initComponents() {

		setBackground(new Color(255, 255, 255));
		setBorder(javax.swing.BorderFactory.createEtchedBorder());

		javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
		this.setLayout(layout);
		layout.setHorizontalGroup(
			layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGap(0, 94, Short.MAX_VALUE)
		);
		layout.setVerticalGroup(
			layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGap(0, 125, Short.MAX_VALUE)
		);
	}
}
