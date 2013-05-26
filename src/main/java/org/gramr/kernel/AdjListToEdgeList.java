package org.gramr.kernel;


import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.gramr.common.RankedAdjList;

public class AdjListToEdgeList extends Configured implements Tool {

	public static class AdjListToEdgeListMapper extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			RankedAdjList adj = RankedAdjList.parseRankedAdjList(value
					.toString());

			for (int dst : adj.getDst())
				context.write(new IntWritable(adj.getSrc()), new IntWritable(
						dst));
		}
	}

	public int printUsage() {
		System.out
				.println("org.gramr.kernel.AdjListToEdgeList inputMatrix outputEdgeList");
		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
		}

		String input = args[0];
		String output = args[1];

		Configuration conf = new Configuration(getConf());
		Job job = null;
		try {
			job = new Job(conf, "Convert AdjList To EdgeList");
			job.setJarByClass(AdjListToEdgeList.class);
			job.setMapperClass(AdjListToEdgeListMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			job.setNumReduceTasks(0);
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
			ToolRunner.run(new AdjListToEdgeList(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
