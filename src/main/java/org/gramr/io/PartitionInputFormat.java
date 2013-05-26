package org.gramr.io;

//import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import org.gramr.common.RankedAdjList;

public class PartitionInputFormat extends
		FileInputFormat<IntWritable, List<RankedAdjList>> {
	public static final Log LOG = LogFactory.getLog(PartitionInputFormat.class);

	@Override
	public RecordReader<IntWritable, List<RankedAdjList>> createRecordReader(
			InputSplit is, TaskAttemptContext tac) {
		return new GraphRecordReader();
	}

	static class GraphRecordReader extends
			RecordReader<IntWritable, List<RankedAdjList>> {
		private IntWritable key;
		private ArrayList<RankedAdjList> value;
		private Configuration conf;
		private FileSplit fileSplit;
		private long start, pos, end;
		private int partitionSize;
		private FSDataInputStream fdin;
		private LineReader in;

		// private BufferedReader br;

		@Override
		public void initialize(InputSplit is, TaskAttemptContext tac) {
			conf = tac.getConfiguration();
			fileSplit = (FileSplit) is;
			pos = start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			partitionSize = conf.getInt("GRAPH_SUBGRAPH_SIZE", 65536);

			try {
				fdin = FileSystem.get(conf).open(fileSplit.getPath());
				fdin.seek(start);
				in = new LineReader(fdin, conf);
				// br = new BufferedReader(new InputStreamReader(fdin));
			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("Couldn't open FileSplit "
						+ fileSplit.getPath().toUri());
			}
		}

		@Override
		public boolean nextKeyValue() throws IOException {
			key = new IntWritable((int) pos);
			value = new ArrayList<RankedAdjList>();

			for (int i = 0; i < partitionSize; i++) {
				if (pos >= end)
					break;
				Text t = new Text();
				int readBytes = in.readLine(t);
				pos += readBytes;

				if (readBytes == 0)
					break;

				RankedAdjList ral = RankedAdjList.parseRankedAdjList(t
						.toString());
				if (ral != null)
					value.add(ral);

			}

			return !value.isEmpty();
		}

		@Override
		public IntWritable getCurrentKey() {
			return key;
		}

		@Override
		public List<RankedAdjList> getCurrentValue() {
			return value;
		}

		public float getProgress() {
			if (start == end)
				return 1.0f;

			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}

		public synchronized void close() throws IOException {
			key = null;
			value.clear();
			value = null;
			fdin.close();
		}
	}
}
