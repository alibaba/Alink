package com.alibaba.alink.operator.common.tree.viz;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;

import javax.imageio.ImageIO;
import javax.imageio.stream.FileImageOutputStream;
import javax.imageio.stream.ImageOutputStream;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TreeModelViz {

	public static void toImageFile(
		String path, List<Row> model, int treeIndex) throws IOException {
		toImageFile(path, model, treeIndex, false);
	}

	public static void toImageFile(
		String path, List<Row> model, int treeIndex, boolean isOverwrite) throws IOException {
		File file = new File(path);

		Preconditions.checkArgument(
			isOverwrite || !file.exists(),
			"File: %s is exists.", path
		);

		toImage(model, treeIndex, new FileImageOutputStream(file));
	}

	public static void toImage(
		List<Row> model, int treeIndex, ImageOutputStream imageOutputStream) throws IOException {
		TreeModelDataConverter modelDataConverter = toModel(model);

		Preconditions.checkArgument(
			treeIndex < modelDataConverter.roots.length && treeIndex >= 0,
			"Index of tree is out of bound. treeIndex: %d", treeIndex
		);

		exportOtherShapesImage(imageOutputStream,
			drawTree2JPanel(
				modelDataConverter.roots[treeIndex],
				modelDataConverter,
				new NodeDimension()
			)
		);
	}

	public static void toImageFile(
		String path, TreeModelDataConverter model, int treeIndex, boolean isOverwrite) throws IOException {
		File file = new File(path);

		Preconditions.checkArgument(
			isOverwrite || !file.exists(),
			"File: %s is exists.", path
		);

		toImage(model, treeIndex, new FileImageOutputStream(file));
	}

	public static void toImage(
		TreeModelDataConverter modelDataConverter, int treeIndex, ImageOutputStream imageOutputStream) throws IOException {

		exportOtherShapesImage(imageOutputStream,
			drawTree2JPanel(
				modelDataConverter.roots[treeIndex],
				modelDataConverter,
				new NodeDimension()
			)
		);
	}

	public static TreeModelDataConverter toModel(List<Row> model) {
		return new TreeModelDataConverter().load(model);
	}

	private static void exportOtherShapesImage(ImageOutputStream stream, JPanel panel) throws IOException {
		Dimension imageSize = panel.getSize();
		BufferedImage image = new BufferedImage(imageSize.width,
			imageSize.height, BufferedImage.TYPE_INT_ARGB);
		Graphics2D g = image.createGraphics();
		panel.paint(g);
		g.dispose();
		ImageIO.write(image, "png", stream);
	}

	private static Tuple2<Double, Double> minMaxPercent(
		TreeModelDataConverter model, Node4CalcPos[] node4CalcPoses) {
		double max = 0.0, min = 1.0;

		if (node4CalcPoses == null
			|| node4CalcPoses.length == 0
			|| model.labels == null) {
			return Tuple2.of(max, min);
		}

		for (int i = 0; i < node4CalcPoses.length; ++i) {
			double multi = NodeJPanel.multiNodeDistribution(node4CalcPoses[i]);
			max = Math.max(multi, max);
			min = Math.min(multi, min);
		}

		return Tuple2.of(Math.min(min, max), Math.max(min, max));
	}

	private static JPanel drawTree2JPanel(
		Node root, TreeModelDataConverter model, NodeDimension nd) {
		JPanel treePanel = new JPanel();

		Tuple3<Integer, Integer, Node4CalcPos[]> range
			= calcNodePos(root, nd.nodeWidth, nd.widthSpace, nd.nodeHigh, nd.heightSpace);

		Tuple2<Double, Double> minMaxPercent = minMaxPercent(model, range.f2);

		for (Node4CalcPos node4CalcPos : range.f2) {
			if (!node4CalcPos.node.isLeaf()) {
				int leftX = range.f2[node4CalcPos.childrenIdx[0]].gNode.posCenter - nd.nodeWidth / 2;
				int leftY = range.f2[node4CalcPos.childrenIdx[0]].gNode.posTop - nd.heightSpace;
				int width = range.f2[node4CalcPos.childrenIdx[node4CalcPos.childrenIdx.length - 1]].gNode.posCenter
					- leftX + nd.nodeWidth / 2;
				int height = nd.heightSpace;
				EdgeJPanel edge = new EdgeJPanel(model, node4CalcPos, range.f2, nd);
				edge.setBounds(leftX, leftY, width, height);
				edge.setFont(nd.geteEdgeFont());
				treePanel.add(edge);
			}

			NodeJPanel jlTemp = new NodeJPanel(model, node4CalcPos, minMaxPercent);
			jlTemp.setBounds(node4CalcPos.gNode.posLeft, node4CalcPos.gNode.posTop, nd.nodeWidth, nd.nodeHigh);
			jlTemp.setFont(nd.getFont());
			treePanel.add(jlTemp);
		}

		treePanel.setSize(new Dimension(range.f0, range.f1));

		return treePanel;
	}

	private static Tuple3<Integer, Integer, Node4CalcPos[]>
	calcNodePos(Node root, double widthNode, double widthSpace, double heightNode, double heightSpace) {
		Node4CalcPos[] nodes = getNodeArray(root);
		double[][] levelRange = subPosition(nodes, widthNode, widthSpace);

		double leftBound = 0;
		double rightBound = 0;
		for (int i = 0; i < levelRange.length; i++) {
			leftBound = Math.min(leftBound, levelRange[i][0]);
			rightBound = Math.max(rightBound, levelRange[i][1]);
		}
		leftBound = Math.floor(leftBound);
		rightBound = Math.ceil(rightBound);

		for (Node4CalcPos tnode : nodes) {
			tnode.gNode = new GraphicNode();
			tnode.gNode.posLevel = tnode.level;
			double cp = tnode.pos - leftBound;
			tnode.gNode.posCenter = (int) cp;
			tnode.gNode.posLeft = (int) (cp - widthNode / 2);
			tnode.gNode.posTop = (int) (tnode.gNode.posLevel * (heightNode + heightSpace));
		}

		return Tuple3.of(
			(int) (rightBound - leftBound),
			(int) Math.ceil(nodes[nodes.length - 1].level * (heightNode + heightSpace) + heightNode),
			nodes
		);
	}

	private static double[][] subPosition(
		Node4CalcPos[] nodes, double widthNode, double widthSpace) {
		ArrayList<double[][]> subTreeLevelRange = new ArrayList<>();
		for (int i = 0; i < nodes.length; i++) {
			subTreeLevelRange.add(new double[0][2]);
		}
		for (int i = nodes.length - 1; i >= 0; i--) {
			if (nodes[i].childrenIdx.length == 0) {
				nodes[i].pos = 0;
				double[][] tempRange = new double[1][2];
				tempRange[0][0] = -widthNode / 2;
				tempRange[0][1] = widthNode / 2;
				subTreeLevelRange.set(i, tempRange);
			} else {
				int nLevel = -1;
				for (int k = 0; k < nodes[i].childrenIdx.length; k++) {
					int childIdx = nodes[i].childrenIdx[k];
					nLevel = Math.max(nLevel, subTreeLevelRange.get(childIdx).length);
				}

				double[][] mergeRange = new double[nLevel][2];
				{
					int childIdx = nodes[i].childrenIdx[0];
					double[][] curRange = subTreeLevelRange.get(childIdx);
					for (int iLevel = 0; iLevel < curRange.length; iLevel++) {
						mergeRange[iLevel][0] = curRange[iLevel][0];
						mergeRange[iLevel][1] = curRange[iLevel][1];
					}
					for (int iLevel = curRange.length; iLevel < nLevel; iLevel++) {
						mergeRange[iLevel][0] = Double.NEGATIVE_INFINITY;
						mergeRange[iLevel][1] = Double.NEGATIVE_INFINITY;
					}
				}
				for (int k = 1; k < nodes[i].childrenIdx.length; k++) {
					int childIdx = nodes[i].childrenIdx[k];
					double[][] curRange = subTreeLevelRange.get(childIdx);
					double minDiff = Double.POSITIVE_INFINITY;
					for (int iLevel = 0; iLevel < curRange.length; iLevel++) {
						if (Double.NEGATIVE_INFINITY != mergeRange[iLevel][1]) {
							minDiff = Math.min(minDiff, curRange[iLevel][0] - mergeRange[iLevel][1]);
						}
					}
					double offset = widthSpace - minDiff;
					nodes[childIdx].pos = offset;
					for (int iLevel = 0; iLevel < curRange.length; iLevel++) {
						if (Double.NEGATIVE_INFINITY == mergeRange[iLevel][1]) {
							mergeRange[iLevel][0] = curRange[iLevel][0] + offset;
							mergeRange[iLevel][1] = curRange[iLevel][1] + offset;
						} else {
							mergeRange[iLevel][1] = curRange[iLevel][1] + offset;
						}
					}
				}

				double s = 0;
				for (int k = 0; k < nodes[i].childrenIdx.length; k++) {
					int childIdx = nodes[i].childrenIdx[k];
					s += nodes[childIdx].pos;
				}
				s /= nodes[i].childrenIdx.length;
				for (int k = 0; k < nodes[i].childrenIdx.length; k++) {
					int childIdx = nodes[i].childrenIdx[k];
					nodes[childIdx].pos -= s;
					moveSubTree(nodes, childIdx, nodes[childIdx].pos);
				}

				nodes[i].pos = 0;

				double[][] tempRange = new double[nLevel + 1][2];
				tempRange[0][0] = -widthNode / 2;
				tempRange[0][1] = widthNode / 2;
				for (int iLevel = 0; iLevel < nLevel; iLevel++) {
					tempRange[iLevel + 1][0] = mergeRange[iLevel][0] - s;
					tempRange[iLevel + 1][1] = mergeRange[iLevel][1] - s;
				}
				subTreeLevelRange.set(i, tempRange);
			}
		}
		return subTreeLevelRange.get(0);
	}

	private static void moveSubTree(
		Node4CalcPos[] nodes, int idxSubTreeRoot, double offset) {
		if (nodes[idxSubTreeRoot].childrenIdx.length > 0) {
			int[] idxSubTree = nodes[idxSubTreeRoot].childrenIdx;
			for (int k : idxSubTree) {
				nodes[k].pos += offset;
				moveSubTree(nodes, k, offset);
			}
		}
	}

	private static Node4CalcPos[] getNodeArray(Node root) {
		ArrayList<Node4CalcPos> nodelist = new ArrayList<Node4CalcPos>();
		Node4CalcPos tnode = new Node4CalcPos();
		tnode.node = root;
		tnode.parentIdx = -1;
		tnode.level = 0;
		nodelist.add(tnode);
		for (int i = 0; i < nodelist.size(); i++) {
			addChildren(nodelist, i);
		}
		return nodelist.toArray(new Node4CalcPos[0]);
	}

	private static void addChildren(ArrayList<Node4CalcPos> nodelist, int idx) {
		if (idx >= nodelist.size()) {
			throw new RuntimeException();
		}
		Node4CalcPos curTreeNode = nodelist.get(idx);
		Node curNode = curTreeNode.node;
		if (!curNode.isLeaf()) {
			int[] idxChildren = new int[curNode.getNextNodes().length];
			int level = curTreeNode.level + 1;
			for (int i = 0; i < idxChildren.length; i++) {
				Node4CalcPos tnode = new Node4CalcPos();
				tnode.node = curNode.getNextNodes()[i];
				tnode.parentIdx = idx;
				tnode.level = level;
				idxChildren[i] = nodelist.size();
				nodelist.add(tnode);
			}
			curTreeNode.childrenIdx = idxChildren;
		}
	}

	static class Node4CalcPos {
		public Node node = null;
		public GraphicNode gNode;

		public int parentIdx = -1;
		public int[] childrenIdx = new int[0];
		public double pos = Double.NaN;
		public int level = -1;
	}

	static class GraphicNode {
		public int posLevel = -1;
		public int posCenter = -1;
		public int posLeft = -1;
		public int posTop = -1;

		public double linepercent = 0;
	}
}
