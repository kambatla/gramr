package org.gramr.io;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.gramr.common.RankedAdjList;

public class RankedAdjListPartitionInputFormat extends
		FileInputFormat<IntWritable, List<RankedAdjList>> {
	public static final Log LOG = LogFactory
			.getLog(RankedAdjListPartitionInputFormat.class);

	@Override
	public RecordReader<IntWritable, List<RankedAdjList>> createRecordReader(
			InputSplit is, TaskAttemptContext tac) {
		return new ExtendedAbstractAdjListPartitionRecordReader();
	}

	class ExtendedAbstractAdjListPartitionRecordReader extends
			AbstractAdjListPartitionRecordReader<RankedAdjList> {

		@Override
		public RankedAdjList readValue(FSDataInputStream fdin)
				throws IOException {
			RankedAdjList value = new RankedAdjList();
			value.readFields(fdin);
			return value;
		}
	}
}
