import java.io.IOException;
import java.util.*;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PMmean {

  public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {

    private Text location = new Text();


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	     MapWritable eachpm = new MapWritable();
       String [] parts = value.toString().split(",");


       String eachkey = parts[0]+parts[1];
       location.set(eachkey);

       if(parts[2].contains("PM2.5")){
         //MapWritable sum = new MapWritable();
         for(int i = 3;i<27;i++){

           //mth = ptn.matcher(parts[i+3]);
           try{
             eachpm.put(new IntWritable(i-3),new IntWritable(Integer.parseInt(parts[i])));
           }
           catch(Exception e){
             eachpm.put(new IntWritable(i-3),new IntWritable(-1));
           }
         }

      context.write(location,eachpm);

	   }

        //context.write(location, sum);

    }

}
 public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {

  public void reduce(Text key, Iterable<MapWritable> values, Context context)
      throws IOException, InterruptedException {

    	//int initialSum = 0;
    	int count = 0;
    	String result = "";
    	//Map<Integer,Integer> sum = new Map<Integer,Integer>();
    	int [] eachpm = new int[24];

      for (MapWritable val : values) {
    		for(int i = 0;i<=23;i++){
    			if(eachpm[i] != 0){
    				int temp = ((IntWritable)val.get(new IntWritable(i))).get();
    				//int pm = temp + sum[i];
    				eachpm[i] = temp;
    			}
    			else{
    				int temp2 = ((IntWritable)val.get(new IntWritable(i))).get();
    				eachpm[i] = temp2;
    			}
    		}
    	}

    	for(int i = 0; i <= 23; i++){


    		result += "," + Integer.toString(eachpm[i]);

    	}


      context.write(key, new Text(result));
    }
 }




 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "PMmean");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(PMmean.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

 }

}
