package org.gramr.kernel;

import java.io.File;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.gramr.common.AdjList;


public class MatrixTranspose extends Configured implements Tool {

	public static class MatrixTransposeMapper extends
			Mapper<IntWritable, AdjList, IntWritable, IntWritable> {
		
		@Override
		public void map(IntWritable key, AdjList adjList, Context context)
				throws IOException, InterruptedException {
			for (int dst : adjList.getDst()) {
				context.write(new IntWritable(dst), new IntWritable(adjList
						.getSrc()));
			}
		}
	}

	public static class MatrixTransposeReducer extends
			Reducer<IntWritable, IntWritable, IntWritable, AdjList> {

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int src = key.get();
			
			Vector<Integer> dstVector = new Vector<Integer>();
			for (IntWritable val : values) {
				dstVector.add(val.get());
			}
			
			context.write(key, AdjList.createAdjList(src, dstVector));
		}
	}

	public int printUsage() {
		System.out.println("org.gramr.kernel.MatrixTranspose inputMatrix outputMatrix");
		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
		}

		String matInputFile = args[0];
		String transposeOutput = args[1];

		Job job = null;
		try {
			job = new Job(getConf(), "Matrix Transpose");
			job.setJarByClass(MatrixTranspose.class);
			job.setMapperClass(MatrixTransposeMapper.class);
			job.setReducerClass(MatrixTransposeReducer.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(matInputFile));
			FileOutputFormat.setOutputPath(job, new Path(transposeOutput));

			job.waitForCompletion(true);
		} finally {
			if (job != null) {
				FileUtil.fullyDelete(new File(job.getConfiguration().get(
						"hadoop.tmp.dir")));
			}
		}

		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new MatrixTranspose(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
