import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageTaxi extends Configured implements Tool {
	
	//Mapper class
	public static class mapper extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context con) throws IOException, InterruptedException{
			String data = value.toString();
			String[] values = data.split(",");
			
			String origin = values[16];		//Reading data from column
			String dest = values[17];		//Reading data from column
			
			String taxiIn = values[19];		//Reading data from column
			String taxiOut = values[20];	//Reading data from column
			
			if(!(taxiOut.equalsIgnoreCase("NA") || taxiOut.equalsIgnoreCase("TaxiOut") || origin.equalsIgnoreCase("Origin") )){
				 con.write(new Text(origin), new Text(taxiOut));
			}
			  
			if(!(taxiIn.equalsIgnoreCase("NA") || taxiIn.equalsIgnoreCase("TaxiIn") || dest.equalsIgnoreCase("Dest") )){
				 con.write(new Text(dest), new Text(taxiIn));
			}
		}
	}
	
	//Reducer class
	public static class reducer extends Reducer<Text, Text, Text, DoubleWritable>{
		
		 TreeSet<RequiredValues> highTaxiTime = new TreeSet<>();	//Treeset to save sorted highest taxi time
		 TreeSet<RequiredValues> lowTaxiTime = new TreeSet<>();	    //Another treeset to save sorted lowest taxi time
		 
		 public static class RequiredValues implements Comparable<RequiredValues>{
			 
			 double avgTime;
			 String airport;
			
			 RequiredValues(double avgTime,String airport){
					this.airport = airport;
					this.avgTime = avgTime;
			}

			public int compareTo(RequiredValues airport2) {
				if(this.avgTime >= airport2.avgTime)
				    return 1;
				else
					return -1;
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException,InterruptedException{
			
			double possibilities = 0;
			int allOccurences =0;
			
			for(Text val : values){
				allOccurences++;
				possibilities = possibilities + Double.parseDouble(val.toString());
			}
			
			double averageTaxi = possibilities / allOccurences;
			RequiredValues result = new RequiredValues(averageTaxi,key.toString());
			lowTaxiTime.add(result);
			highTaxiTime.add(result);
			
			if(lowTaxiTime.size()>3){	//finding only 3 airlines
				lowTaxiTime.pollLast();
			}
			if(highTaxiTime.size()>3){	//finding only 3 airlines
				highTaxiTime.pollFirst();
			}
		}
		
		protected void cleanup(Context con) throws IOException, InterruptedException {
			 
			con.write(new Text("Min Taxi"), null);
	   	  	while (!lowTaxiTime.isEmpty()) {
	   	  			RequiredValues result = lowTaxiTime.pollFirst();
		            con.write(new Text(result.airport), new DoubleWritable(result.avgTime));
	         }
	   	  	con.write(new Text("Max Taxi"), null);
			while (!highTaxiTime.isEmpty()) {
					RequiredValues result = highTaxiTime.pollLast();
		            con.write(new Text(result.airport), new DoubleWritable(result.avgTime));
		          }
			}
	}

	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf(), "AverageTaxi");
		job.setJarByClass(getClass());

		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);

		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	//Main method
	public static void main(String[] args) throws Exception{
		
		int end = ToolRunner.run(new AverageTaxi(), args);
		System.exit(end);
		
	}
}
