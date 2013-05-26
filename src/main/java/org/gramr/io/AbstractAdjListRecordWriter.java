package org.gramr.io;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.gramr.common.AbstractAdjList;

public class AbstractAdjListRecordWriter<ADJ extends AbstractAdjList> extends
		RecordWriter<IntWritable, ADJ> {
	protected DataOutputStream out;
	
	AbstractAdjListRecordWriter(DataOutputStream out) {
		this.out = out;
	}

//	protected abstract void writeValue(ADJ value);
	
	public synchronized void write(IntWritable key, ADJ value) throws IOException {
		value.write(out);
	}

	public synchronized void close(TaskAttemptContext context)
			throws IOException {
		out.close();
	}
}
