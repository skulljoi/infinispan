package org.infinispan.hadoopintegration.sample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;

import static java.nio.file.Files.readAllBytes;

public class MapReducerTest {

   private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
   private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

   @Before
   public void setUp() {
      MapClass mapClass = new MapClass();
      ReduceClass reduceClass = new ReduceClass();
      mapDriver = MapDriver.newMapDriver(mapClass);
      reduceDriver = ReduceDriver.newReduceDriver(reduceClass);
   }


   @Test
   public void testMapper() throws Exception {
      mapDriver.withInput(new LongWritable(0), new Text(getFileContents("bar.txt")));
      mapDriver.withOutput(new Pair<>(new Text("the"), new IntWritable(1)));
      mapDriver.withOutput(new Pair<>(new Text("leffe"), new IntWritable(1)));
      mapDriver.withOutput(new Pair<>(new Text("beer"), new IntWritable(1)));
      mapDriver.withOutput(new Pair<>(new Text("is"), new IntWritable(1)));
      mapDriver.withOutput(new Pair<>(new Text("the"), new IntWritable(1)));
      mapDriver.withOutput(new Pair<>(new Text("best"), new IntWritable(1)));
      mapDriver.runTest();

   }

   @Test
   public void testReducer() throws Exception {

      IntWritable one = new IntWritable(1);
      IntWritable two = new IntWritable(2);
      reduceDriver.withInput(new Text("the"), Arrays.asList(one, one));

      reduceDriver.withOutput(new Pair<>(new Text("the"), two));

      reduceDriver.runTest();


   }

   private String getFileContents(String path) throws IOException, URISyntaxException {
      URI uri = this.getClass().getClassLoader().getResource(path).toURI();
      return new String(readAllBytes(Paths.get(uri)));
   }

}
