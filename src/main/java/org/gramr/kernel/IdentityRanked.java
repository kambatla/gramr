package org.gramr.kernel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gramr.common.AdjList;
import org.gramr.common.RankedAdjList;
import org.gramr.io.AdjListInputFormat;
import org.gramr.io.RankedAdjListInputFormat;

import java.io.File;
import java.io.IOException;

public class IdentityRanked extends Configured implements Tool {

	public static class RankedIdentityMapper extends
			Mapper<IntWritable, RankedAdjList, IntWritable, RankedAdjList> {

		public void map(IntWritable key, RankedAdjList adj, Context context)
				throws IOException, InterruptedException {
      context.write(new IntWritable(adj.getSrc()), adj);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		String input = args[0];
		String output = args[1];

		Job job = null;
		try {
			job = new Job(conf, "IdentityRanked");
			job.setJarByClass(IdentityRanked.class);

			job.setInputFormatClass(RankedAdjListInputFormat.class);

			job.setMapperClass(RankedIdentityMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(RankedAdjList.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(RankedAdjList.class);
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
			ToolRunner.run(new Configuration(), new IdentityRanked(),
					args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
