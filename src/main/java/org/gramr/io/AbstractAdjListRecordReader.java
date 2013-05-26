package org.gramr.io;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.gramr.common.AbstractAdjList;

public abstract class AbstractAdjListRecordReader<ADJ extends AbstractAdjList>
		extends RecordReader<IntWritable, ADJ> {
	private IntWritable key;
	private ADJ value;
	private Configuration conf;
	private FileSplit fileSplit;
	private long start, pos, end;
	private FSDataInputStream fdin;

	@Override
	public void initialize(InputSplit is, TaskAttemptContext tac) {
		conf = tac.getConfiguration();
		fileSplit = (FileSplit) is;
		pos = start = fileSplit.getStart();
		end = start + fileSplit.getLength();

		try {
			fdin = FileSystem.get(conf).open(fileSplit.getPath());
			fdin.seek(start);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected abstract ADJ readValue(FSDataInputStream fdin) throws IOException;

	@Override
	public boolean nextKeyValue() throws IOException {
		key = null;
		value = null;
		pos = fdin.getPos();
		if (pos >= end) {
			return false;
		}

		try {
			value = readValue(fdin);
		} catch (EOFException eof) {
			value = null;
			return false;
		}
		key = new IntWritable((int) pos);
		return true;
	}

	@Override
	public IntWritable getCurrentKey() {
		return key;
	}

	@Override
	public ADJ getCurrentValue() {
		return value;
	}

	public float getProgress() {
		if (start == end)
			return 1.0f;

		return Math.min(1.0f, (pos - start) / (float) (end - start));
	}

	public synchronized void close() throws IOException {
		key = null;
		value = null;
		fdin.close();
	}
}