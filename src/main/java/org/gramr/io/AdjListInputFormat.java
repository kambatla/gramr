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

import org.gramr.common.AdjList;

public class AdjListInputFormat extends
		FileInputFormat<IntWritable, AdjList> {
	public static final Log LOG = LogFactory.getLog(AdjListInputFormat.class);

	@Override
	public RecordReader<IntWritable, AdjList> createRecordReader(
			InputSplit is, TaskAttemptContext tac) {
		return new ExtendedAbstractAdjListRecordReader();
	}
	
	class ExtendedAbstractAdjListRecordReader extends AbstractAdjListRecordReader<AdjList> {

		@Override
		public AdjList readValue(FSDataInputStream fdin) throws IOException{
			AdjList value = new AdjList();
			value.readFields(fdin);
			return value;
		}		
	}
 }
