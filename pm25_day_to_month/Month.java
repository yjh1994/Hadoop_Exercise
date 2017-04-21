import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Month {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text KeyWord = new Text();
    private Text ValueWord = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
	String [] parts = line.split(",");
        //StringTokenizer tokenizer = new StringTokenizer(line);
        //while (tokenizer.hasMoreTokens()) {
        // word.set(tokenizer.nextToken());
	
	int month = Integer.valueOf(parts[0].substring(5,7));
	
	//int month = Integer.parseInt(line.substring(5,7));
	System.out.println("test");
	String season;
	
	
	/*
	if(month>=3 && month <=5){
		season = "a";
	}
	else if(month>=6 && month <=8){
		season = "b";

	}
	else if(month >=9 && month <= 11){
		season = "c";
	}
	else{
		season = "d";
	}
	*/

	if(month<=12){
		String temp = parts[1]+Integer.toString(month);
	
	
	//String temp = parts[1]+Integer.toString(month);
	KeyWord.set(temp);
	ValueWord.set(line);
        context.write(KeyWord , ValueWord);
	}
       // }
    }
 } 
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {
	
    private Text output = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        //int sum = 0;
	int temp = 0;
	String [] parts = new String[27];
	int [] sum =new int [24];
	int [] seasonAVG = new int [24];
	int times = 0;
	String result ="";

        for (Text val : values) {

		parts = val.toString().split(",");
		if(times == 0){
			for(int i = 0;i<24;i++){
				sum[i] = Integer.parseInt(parts[i+3]); 
			}
		}
		else{
			for(int j =0 ; j<24; j++){
				sum[j] += Integer.parseInt(parts[j+3]);
			}
			
		}
		times++;
        }
	
	for(int k = 0 ;k<24;k++){
		 seasonAVG[k] =  sum[k]/times;
	}

	result = parts[1]+","+parts[2];

	for(int p = 0; p<24;p++){
		result += "," + Integer.toString(seasonAVG[p]); 
	}
	
	String outkey = key.toString() + ",";

	output.set(result);
        context.write(new Text(outkey), output);
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "Month");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(Month.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
