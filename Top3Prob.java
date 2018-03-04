import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Top3Prob {

	public static TreeSet<RequiredValues> low_top3 = new TreeSet<>();	//Treeset to save sorted lowest probability for being on schedule
	public static TreeSet<RequiredValues> high_top3 = new TreeSet<>();	//Treeset to save sorted highest probability for being on schedule
	
	//Mapper class
	//Delayed time is set to 12 minutes
	
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			
			String values[] = value.toString().split(",");
			
			String airlines = values[8];	//Reading data from column
			String delay1 = values[14];		//Reading data from column
			String delay2 = values[15];		//Reading data from column

			if (IntegerCheck(delay1)) {
				con.write(new Text(airlines+"_Total"), new LongWritable(1));
				if (Integer.parseInt(delay1) <= 12) {
					con.write(new Text(airlines), new LongWritable(1));
				}
			}
			if (IntegerCheck(delay2)) {
				con.write(new Text(airlines+"_Total"), new LongWritable(1));
				if (Integer.parseInt(delay2) <= 12) {
					con.write(new Text(airlines), new LongWritable(1));
				}
			}
		}
		
		public static boolean IntegerCheck(String str) {
			
			boolean integerFlag = false;
			try {
				Integer.parseInt(str);
				integerFlag = true;
			} catch (NumberFormatException ex) {
			}
			return integerFlag;
		}
	}
	
	//Combiner class
	public static class Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {

			long occurence = 0;
			for (LongWritable value : values) {
				occurence += value.get();
			}
			con.write(key, new LongWritable(occurence));
		}
	}
	
	//Reducer class
	public static class Reduce extends Reducer<Text, LongWritable, Text, DoubleWritable> {
		private DoubleWritable allOccurences = new DoubleWritable();
		private DoubleWritable onTime = new DoubleWritable();
		
		private Text presentAirlines = new Text("B_L_A_N_K");
		
		public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {
			if(key.toString().equalsIgnoreCase(presentAirlines.toString()+"_Total")){
				allOccurences.set(findAllOccurences(values));
				
				double scheduledTime=onTime.get()/allOccurences.get();
				String airlinekey=presentAirlines.toString();
				
				low_top3.add(new RequiredValues(airlinekey,scheduledTime));
				high_top3.add(new RequiredValues(airlinekey,scheduledTime));
				
				if(low_top3.size()>3){
					low_top3.pollFirst();
				}
				if(high_top3.size()>3){
					high_top3.pollLast();
				}
			}else{
				presentAirlines.set(key.toString());
				onTime.set(findAllOccurences(values));
			}
			
		}
		
		private double findAllOccurences(Iterable<LongWritable> values) {
			
			double occurences = 0;
			for (LongWritable occurrence : values) {
				occurences += occurrence.get();
			}
			return occurences;
		}
		
		public static class RequiredValues implements Comparable<RequiredValues> {
			
			String airlines;
			double onTime_prob;
			
			RequiredValues(String airlines, double onTime_prob) {
				this.onTime_prob = onTime_prob;
				this.airlines = airlines;
			}
			
			public int compareTo(RequiredValues RequiredValues) {
				if (this.onTime_prob <= RequiredValues.onTime_prob) {
					return 1;
				} else {
					return -1;
				}
			}
		}
		
		protected void cleanup(Context con) throws IOException, InterruptedException {
			con.write(new Text("Top 3 probability output:" ), null);
			 while (!high_top3.isEmpty()) {
	    		  RequiredValues RequiredValues = high_top3.pollFirst();	//finding only 3 airlines
	              con.write(new Text(RequiredValues.airlines), new DoubleWritable(RequiredValues.onTime_prob));
	          }
			 con.write(new Text("Low 3 probability output:" ), null );
	    	  while (!low_top3.isEmpty()) {
	    		  RequiredValues RequiredValues = low_top3.pollLast();	//finding only 3 airlines
	              con.write(new Text(RequiredValues.airlines), new DoubleWritable(RequiredValues.onTime_prob));
	          }
	    }
	}

	//Main method
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(Top3Prob.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combiner.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.waitForCompletion(true);
	}
}
