package org.infinispan.hadoopintegration.sample;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class HDFSMain {

   public static void main(String[] args) throws IOException {
      JobConf jobConf = new JobConf(HDFSMain.class);
      jobConf.setJobName("wordcount");

      jobConf.setOutputKeyClass(Text.class);
      jobConf.setOutputValueClass(IntWritable.class);

      jobConf.setMapperClass(MapClass.class);
      jobConf.setReducerClass(ReduceClass.class);

      FileInputFormat.addInputPath(jobConf, new Path(args[0]));
      FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
      System.out.println("About to run the job!!!!");
      JobClient.runJob(jobConf);

      System.out.println("Finished executing job.");

   }
}
