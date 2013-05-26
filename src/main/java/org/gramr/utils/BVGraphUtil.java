package org.gramr.utils;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;

import org.gramr.common.AdjList;
import org.gramr.common.RankedAdjList;

/*
 * BVGraphUtil: wrapper for webgraph datasets
 * 
 */

public class BVGraphUtil {
	public static final Log LOG = LogFactory.getLog(BVGraphUtil.class);

	static class Range {
		private final static int falseStart = -1;
		private int start, end;

		Range(int from, int to) {
			start = from;
			end = to;
		}

		public int getStart() {
			return start;
		}

		public int getEnd() {
			return end;
		}

		public boolean hasNode(int val) {
			return (start <= val && val <= end);
		}
	}

	private BVGraph graph;
	private NodeIterator iter, riter;
	private Range[] splitRanges;
	private Range currentRange;

	public BVGraphUtil(String path) throws IOException {
		graph = BVGraph.load(path);
		iter = graph.nodeIterator();
	}

	public int getNumNodes() {
		return graph.numNodes();
	}

	public long getNumEdges() {
		return graph.numArcs();
	}

	public boolean hasMoreNodes() {
		return iter.hasNext();
	}

	public AdjList getNextAdjList() {
		NodeIterator currIter = (riter == null) ? iter : riter;
		if (!currIter.hasNext())
			return null;

		int src = currIter.nextInt();
		if (riter != null && !currentRange.hasNode(src))
			return null;

		LazyIntIterator successors = currIter.successors();
		ArrayList<Integer> dst = new ArrayList<Integer>();

		while (true) {
			int next = successors.nextInt();
			if (next == -1)
				break;
			dst.add(next);
		}

		return AdjList.createAdjList(src, dst);
	}

	public RankedAdjList getNextRankedAdjList() {
		AdjList adj = getNextAdjList();
		if (adj == null)
			return null;
		return RankedAdjList.createRankedAdjList(adj.getSrc(), 1.0f, adj
				.getDst());
	}

	public void readRange(int i) {
		currentRange = splitRanges[i];
		LOG
				.info("readRange() = " + currentRange.start + " "
						+ currentRange.end);
		riter = graph.nodeIterator(currentRange.start);
	}

	public int getRangeIndex(int node) {
		for (int i = 0; i < splitRanges.length; i++)
			if (splitRanges[i].hasNode(node))
				return i;

		return -1;
	}

	/*
	 * @return Range[]: each element denoting one range
	 */
	public boolean splitVerticesEqually(int numSplits) {
		int n = getNumNodes();

		/* case0: numNodes == 0 */
		if (n == 0)
			return false;

		Range[] splits = new Range[numSplits];

		/* case1: numSplits == 0 */
		if (numSplits == 0)
			return false;

		/* case 2: */

		int splitSize = n / numSplits;
		int spill = n % numSplits;

		NodeIterator niter = graph.nodeIterator();
		for (int i = 0; i < numSplits; i++) {
			int start = Range.falseStart, end;

			if (niter.hasNext())
				start = niter.nextInt();

			// skip appropriate number of vertices
			int skip = splitSize - 1;
			if (spill > 0) {
				skip--;
				spill--;
			}
			niter.skip(skip);

			if (niter.hasNext())
				end = niter.nextInt();
			else {
				/* case when there aren't enough nodes to fill the split */
				niter = graph.nodeIterator(start);
				end = start;
				while (niter.hasNext())
					end = niter.nextInt();
			}
			splits[i] = new Range(start, end);
		}

		splitRanges = splits;
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}
}
