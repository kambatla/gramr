package org.gramr.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.gramr.common.RankedAdjList;

public class RankedAdjListInputFormat extends
		FileInputFormat<IntWritable, RankedAdjList> {
	public static final Log LOG = LogFactory.getLog(RankedAdjListInputFormat.class);

	@Override
	public RecordReader<IntWritable, RankedAdjList> createRecordReader(
			InputSplit is, TaskAttemptContext tac) {
		return new ExtendedAbstractAdjListRecordReader();
	}
	
	class ExtendedAbstractAdjListRecordReader extends AbstractAdjListRecordReader<RankedAdjList> {

		@Override
		public RankedAdjList readValue(FSDataInputStream fdin) throws IOException{
			RankedAdjList value = new RankedAdjList();
			value.readFields(fdin);
			return value;
		}		
	}
 }
