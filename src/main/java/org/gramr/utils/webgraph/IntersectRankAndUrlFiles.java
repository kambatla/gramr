package org.gramr.utils.webgraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class IntersectRankAndUrlFiles extends Configured implements Tool{

  public static enum UrlIntersection {
    Common, Diff
  }

  public static class EmitUrl extends
      Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      RankAndUrl rankAndUrl = RankAndUrl.parse(value.toString());
      context.write(new Text(rankAndUrl.getUrl()), new IntWritable(1));
    }
  }

  public static class UrlMatch extends
      Reducer<Text, IntWritable, NullWritable, Text> {
    public void reduce(Text src, Iterable<IntWritable> nums,
                       Context context) throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable num : nums) {
        count += num.get();
      }
      if (count == 1) {
        context.getCounter(UrlIntersection.Diff).increment(1);
        context.write(NullWritable.get(), src);
      } else {
        context.getCounter(UrlIntersection.Common).increment(1);
      }
    }
  }

  private static int printUsage() {
    System.out
        .println("Usage: org.gramr.kernel.IntersectRankAndUrlFiles <Urls " +
            "files> <diff urls>");
    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      printUsage();
      System.exit(0);
    }

    Configuration conf = new Configuration(getConf());

    String urlsFile = args[0];
    String output = args[1];

    Job job = null;
    try {
      job = new Job(conf, "Intersect RankAndUrl files");
      job.setJarByClass(IntersectRankAndUrlFiles.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setMapperClass(EmitUrl.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setReducerClass(UrlMatch.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(urlsFile));
      FileOutputFormat.setOutputPath(job, new Path(output));

      job.setNumReduceTasks(new JobClient(new JobConf(conf)).getClusterStatus
          ().getMaxReduceTasks());
      job.waitForCompletion(true);
    } finally {
      if (job != null) {
        FileUtil.fullyDelete(new File(job.getConfiguration().get(
            "hadoop.tmp.dir")));
      }
    }

    return 0;
  }

  public static void main(String[] args) {
    try {
      ToolRunner.run(new Configuration(), new IntersectRankAndUrlFiles(), args);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }
}
