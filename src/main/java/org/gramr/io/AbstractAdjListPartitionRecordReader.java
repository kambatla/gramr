package org.gramr.io;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.gramr.common.AbstractAdjList;

public abstract class AbstractAdjListPartitionRecordReader<ADJ extends AbstractAdjList>
		extends RecordReader<IntWritable, List<ADJ>> {
	private IntWritable key;
	private List<ADJ> value;
	private Configuration conf;
	private FileSplit fileSplit;
	private long start, pos, end;
	private FSDataInputStream fdin;
	private int partitionSize;

	@Override
	public void initialize(InputSplit is, TaskAttemptContext tac) {
		conf = tac.getConfiguration();
		partitionSize = conf.getInt("GRAPH_PARTITION_SIZE", 65536);
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
		key = new IntWritable((int) pos);
		value = new ArrayList<ADJ>();

		for (int i = 0; i < partitionSize; i++) {
			pos = fdin.getPos();
			if (pos >= end)
				break;
			ADJ val = null;
			try {
				val = readValue(fdin);
			} catch (EOFException eof) {
				val = null;
			}
			if (val != null) {
				value.add(val);
			}
		}

		if (value.isEmpty()) {
			key = null;
			value = null;
			return false;
		} else
			return true;
	}

	@Override
	public IntWritable getCurrentKey() {
		return key;
	}

	@Override
	public List<ADJ> getCurrentValue() {
		return value;
	}

	public float getProgress() {
		if (start == end)
			return 1.0f;

		return Math.min(1.0f, (pos - start) / (float) (end - start));
	}

	public synchronized void close() {
	}
}