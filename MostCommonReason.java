import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.TreeSet;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MostCommonReason  {
	
	public static TreeSet<RequiredValues> reasonsSet = new TreeSet<>();	//Treeset to save sorted reason values
	
	//Mapper class
	public static class mapper extends Mapper<Object, Text, Text, IntWritable>{
		
		public void map(Object key, Text value, Context con) throws IOException, InterruptedException{
			
			String data = value.toString();
			String[] values = data.split(",");
			
			String reasonCancellation = values[22];	//Reading data from column
			
			if((reasonCancellation.equalsIgnoreCase("A") || reasonCancellation.equalsIgnoreCase("B")|| reasonCancellation.equalsIgnoreCase("C")|| reasonCancellation.equalsIgnoreCase("D") ))
		     	con.write(new Text(reasonCancellation), new IntWritable(1));	
		}	
	}
	
	public static class RequiredValues implements Comparable<RequiredValues>{
		
		String cancellationReason;
		int total;
		
		RequiredValues(String cancellationReason, int total){
			this.cancellationReason = cancellationReason;
			this.total = total;
		}

		public int compareTo(RequiredValues anotherCode) {
			if(this.total >= anotherCode.total)
				return 1;
			else
				return -1;
		}
	}

	//Reducer class
	public static class reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values , Context con) throws IOException, InterruptedException {
			
			int total = 0;
			for(IntWritable val1 : values){
				total = total + val1.get();
			}
			reasonsSet.add( new RequiredValues(key.toString(), total) );
			
			if(reasonsSet.size() > 150){
				reasonsSet.pollFirst();
			}
		}
			
      protected void cleanup(Context con) throws IOException, InterruptedException {
    	  
    	  while (!reasonsSet.isEmpty()) {
			  RequiredValues code = reasonsSet.pollLast();
              con.write(new Text(code.cancellationReason), new IntWritable(code.total));
          }
		}
	}
	
	//Main method
	public static void main(String[] args)throws Exception {
		
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(MostCommonReason.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(mapper.class);
		job.setCombinerClass(reducer.class);
		job.setReducerClass(reducer.class);

		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}

