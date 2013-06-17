package org.gramr.utils.webgraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

import java.io.File;
import java.io.IOException;

public class SortAndFilterRankAndUrl extends Configured implements Tool {

  public static class FilterOutDynamicUrls extends
      Mapper<LongWritable, Text, RankAndUrl, NullWritable> {

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      RankAndUrl rankAndUrl = RankAndUrl.parse(value.toString());
      String url = rankAndUrl.getUrl();
      if (url != null && !url.contains("?")) {
        context.write(rankAndUrl, NullWritable.get());
      }
    }
  }

  public static class SortRankAndUrl extends
      Reducer<RankAndUrl, NullWritable, NullWritable, Text> {
    public void reduce(RankAndUrl src, Iterable<RankAndUrl> values,
                       Context context) throws IOException, InterruptedException {
      context.write(NullWritable.get(), new Text(src.toString()));
    }
  }

  private static int printUsage() {
    System.out
        .println("Usage: org.gramr.kernel.SortRankedGraphByRank <Graph> <SortedVector>");
    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      printUsage();
      System.exit(0);
    }

    Configuration conf = new Configuration(getConf());

    String input = args[0];
    String output = args[1];

    Job job = null;
    try {
      job = new Job(conf, SortAndFilterRankAndUrl.class.getName());
      job.setJarByClass(SortAndFilterRankAndUrl.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setMapperClass(FilterOutDynamicUrls.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(RankAndUrl.class);
      job.setReducerClass(SortRankAndUrl.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(input));
      FileOutputFormat.setOutputPath(job, new Path(output));

      job.setNumReduceTasks(1);
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
      ToolRunner.run(new Configuration(), new SortAndFilterRankAndUrl(), args);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

}
