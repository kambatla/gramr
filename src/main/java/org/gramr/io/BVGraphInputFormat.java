package org.gramr.io;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.gramr.utils.BVGraphUtil;

import org.gramr.common.AdjList;

public class BVGraphInputFormat extends
		FileInputFormat<IntWritable, AdjList> {
	public static final Log LOG = LogFactory.getLog(BVGraphInputFormat.class);

	@Override
	public RecordReader<IntWritable, AdjList> createRecordReader(
			InputSplit is, TaskAttemptContext tac) {
		return new BVGraphRecordReader();
	}

	static class BVGraphRecordReader extends
			RecordReader<IntWritable, AdjList> {
		private Configuration conf;
		private FileSplit fileSplit;
		private BVGraphUtil graph;
		private int numNodes, numSplits, count = 0;
		
		private IntWritable key;
		private AdjList value;
		
		@Override
		public void initialize(InputSplit is, TaskAttemptContext tac) {
			conf = tac.getConfiguration();
			fileSplit = (FileSplit) is;
			try {
				graph = new BVGraphUtil(fileSplit.getPath().toString());
				
			} catch (IOException e) {
				LOG.error("could not open file");
				e.printStackTrace();				
			}
			
			numNodes = graph.getNumNodes();
			numSplits = conf.getInt("BV_GRAPH_NUM_SPLITS", 32);
			graph.splitVerticesEqually(numSplits);
		}

		@Override
		public boolean nextKeyValue() throws IOException {
			if (!graph.hasMoreNodes())
				return false;
			
			value = graph.getNextAdjList();
			key = new IntWritable(graph.getRangeIndex(value.getSrc()));
			count++;
			return true;
		}

		@Override
		public IntWritable getCurrentKey() {
			return key;
		}

		@Override
		public AdjList getCurrentValue() {
			return value;
		}

		public float getProgress() {
			return count/numNodes;
		}

		public synchronized void close() throws IOException {
			key = null;
			value = null;
		}
	}
}
