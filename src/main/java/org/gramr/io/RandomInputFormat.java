package org.gramr.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * Random Input Format: 
 * creates a given number of splits
 * #splits passed through configuration parameter "RANDOM_INPUT_FORMAT_NUM_MAPS"
 */

public class RandomInputFormat extends InputFormat<Text, Text> {
	private Log LOG = LogFactory.getLog(RandomInputFormat.class);
	public static final String NUM_SPLITS = "RANDOM_INPUT_FORMAT_NUM_MAPS";

	@Override
	public List<InputSplit> getSplits(JobContext jc) throws IOException,
			InterruptedException {
		ArrayList<InputSplit> alis = new ArrayList<InputSplit>();
		Configuration conf = jc.getConfiguration();
		int numMaps = conf.getInt("RANDOM_INPUT_FORMAT_NUM_MAPS", -1);
		if (numMaps < 0) {
			LOG
					.error("Number of maps is either not set or set to less than zero");
			return alis;
		}

		for (int i = 0; i < numMaps; i++) {
			alis.add(new FileSplit(new Path("dummy-split-" + i), 0, 1,
					(String[]) null));
		}
		return alis;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit is,
			TaskAttemptContext tac) throws IOException, InterruptedException {
		return new RandomRecordReader();
	}

	static class RandomRecordReader extends RecordReader<Text, Text> {
		private Text key;
		private Text value;
		private boolean nextKeyValue;

		@Override
		public void initialize(InputSplit is, TaskAttemptContext tac) {
			String fileName = ((FileSplit) is).getPath().getName();
			String[] fileNameSplits = fileName.split("-");
			this.key = new Text(fileNameSplits[2]);
			this.value = new Text(fileNameSplits[2]);
			this.nextKeyValue = true;
		}

		@Override
		public boolean nextKeyValue() throws IOException {
			if (this.nextKeyValue) {
				this.nextKeyValue = false;
				return true;
			} else {
				return false;
			}
		}

		@Override
		public Text getCurrentKey() {
			return key;
		}

		@Override
		public Text getCurrentValue() {
			return value;
		}

		public float getProgress() {
			if (nextKeyValue)
				return (float) 0.0;
			else
				return (float) 1.0;
		}

		public synchronized void close() {
		}
	}
}
