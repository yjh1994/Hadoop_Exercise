import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

public class PM25Kmean {
	// init output path
	private static String OUT = "";
	// init input path
	private static String IN = "";

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		//keyWord => context write key
		private Text keyWord = new Text();
		//valueWord => context write value
		private Text valueWord = new Text();
		// setupData => input external centers dataset
		private String setupData = "";
		// centers => init array for each center
		private int[][] centers;
		// k_num => how many k?
		private int k_num = 0;

		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("========== in Mapper setup ==========");
			// conf, fs, cacheFiles => get main function Configuration setting
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			// cacheReader => read cacheFiles every line
			BufferedReader cacheReader;

			// init k_num
			k_num = Integer.parseInt(conf.get("k_num"));
			// init centers
			centers = new int[k_num][24];


			/* start to get input centers dataset */
			if(cacheFiles != null && cacheFiles.length > 0){
					cacheReader = new BufferedReader(
						new FileReader(cacheFiles[cacheFiles.length - 1].toString()));
				try{
					System.out.println("========= input file to array ===========");
					setupData = cacheReader.readLine().replace("	", "");
					for(int i = 0; i< k_num; i++){
						int countTest = 0;
						for(String splits: setupData.split(",")){

							centers[i][countTest] = Integer.parseInt(splits.replace(" ", ""));
							System.out.print(" " + centers[i][countTest]);
							countTest = countTest + 1;
						}
						System.out.println();
						setupData = cacheReader.readLine().replace("	", "");
					}

					System.out.println("========= input file to array success ===========");
				}
				catch (Exception e){
					e.printStackTrace();
					System.out.println("\n================= >>> \n" + setupData);
				}

			}

		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			System.out.println("===== map function process =====");
			//line => text value to String
			String line = value.toString();
			String[] lineList = line.split(",");
			//store each distance between each center and each node
			int distances[] = new int[k_num];
			//minDist => init min distance
			int minDist = 10000;
			//finalCluster => final neighbor center
			String finalCluster = "";

			// init info
			String info = "";
			// info => date + location
			info = lineList[0] + lineList[1];

			//calculate each distance
			for(int i = 0; i < k_num; i++){
				for(int j = 3; j < 27; j++){
					distances[i] = distances[i] + Math.abs(Integer.parseInt(lineList[j]) - centers[i][j - 3]);
				}
				if(distances[i] < minDist){
					minDist = distances[i];
					finalCluster = Integer.toString(i);
				}
			}
			// keyWord set finalCluster
			keyWord.set(finalCluster);
			// valueWord set 24 hour value
			valueWord.set(Arrays.toString(lineList).replace(" ", "").replace("]", ""));

			context.write(keyWord, valueWord);
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context)
		    throws IOException, InterruptedException{

			int count = 0;
			int[] calTest = new int[24];
			for(Text info: values){
				String test = "";
				String allInfo = info.toString();
				String[] hourInfo = allInfo.split(",");
				for(int i = 3; i < 27; i++){

					calTest[i - 3] = calTest[i - 3] + Integer.parseInt(hourInfo[i]);
				}
				count = count + 1;

			}
			for(int i = 0; i< calTest.length; i++){
				calTest[i] = calTest[i] / count;
			}
			context.write(new Text(), new Text(Arrays.toString(calTest).replace("[", "").replace("]", "")));
		}
	}


	// final iteration export location, date, center
	// example: XX, xxxx/xx/xx, 0
	public static class FinalReduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context)
		    throws IOException, InterruptedException{

			for(Text info: values){
				String[] hourInfo = info.toString().split(",");
				StringBuilder infoDetailBuilder = new StringBuilder();
				infoDetailBuilder.append(hourInfo[0].replace("[", "")).append(",");
				infoDetailBuilder.append(hourInfo[1]).append(",");

				context.write(new Text(infoDetailBuilder.toString()), key);
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int iteration = 0;
		IN = args[0];
		OUT = args[1];
		conf.set("k_num", args[2]);
		String output = OUT + Integer.toString(iteration);
		String prevOutput = "";

		//setting distributed cache & set iteration
		while(iteration < 3){
			conf.set("iteration", Integer.toString(iteration));
			if(iteration == 0){
				Path hdfsPath = new Path("center.txt");
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			}
			else{
				String temp = "";
				Path hdfsPath = new Path(prevOutput + "/part-r-00000");
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			}
			Job job = new Job(conf, "PM25Kmean");

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			if(iteration < 2){
				job.setReducerClass(Reduce.class);
			}
			else{
				job.setReducerClass(FinalReduce.class);
			}
			job.setJarByClass(PM25Kmean.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(IN));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);


			System.out.println("==========Test Pass Parameter==========\n");

			iteration = iteration + 1;
			prevOutput = output;
			output = OUT + Integer.toString(iteration);
		}
	}


}
