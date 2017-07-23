import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



//Mapper Class, Counts words in each line. For each line, break the line into words and emits them as (word, 1)

public class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();
 
 @Override
 public void map(LongWritable arg0, Text value, OutputCollector<Text, IntWritable> output, Reporter arg3)
 		throws IOException {
 
  String line = value.toString();
  StringTokenizer itr = new StringTokenizer(line);
  while (itr.hasMoreTokens()) {
    word.set(itr.nextToken());
     output.collect(word, one);
   }
 }
//


	// TODO Auto-generated method stub
	
}

