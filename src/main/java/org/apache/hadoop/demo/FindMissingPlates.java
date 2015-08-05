package org.apache.hadoop.demo;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.demo.WordCount.Map;
import org.apache.hadoop.demo.WordCount.Reduce;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class FindMissingPlates {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private final static IntWritable TWO = new IntWritable(2);
    private final static IntWritable THREE = new IntWritable(3);
    private final static String LICENSE_PLATES = "file01.txt";
    private final static String LICENSE_PLATES_PIPELINE = "file02.txt";
    private final static String LICENSE_PLATES_STREAM = "file03.txt";

    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      FileSplit fsplit = (FileSplit) reporter.getInputSplit();
      String inputFileName = fsplit.getPath().getName();
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      String token = tokenizer.nextToken();
      word.set(token);
      if (inputFileName.equalsIgnoreCase(LICENSE_PLATES))
        output.collect(word, ONE);
      else if (inputFileName.equalsIgnoreCase(LICENSE_PLATES_PIPELINE))
        output.collect(word, TWO);
      else if (inputFileName.equalsIgnoreCase(LICENSE_PLATES_STREAM))
        output.collect(word, THREE);
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws IOException {
    JobConf conf = new JobConf(WordCount.class);
    conf.setJobName("license-plates");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf, new Path(args[2]));

    JobClient.runJob(conf);
  }

}
