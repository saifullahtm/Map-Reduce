package AkshayMapReduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.math3.util.IterationEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.dataNodeHome_jsp;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver
{
	StringBuilder str = new  StringBuilder();
	public static void main(String[] args) throws Exception
	{
		
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "NYPD Analyse");
		job.setJarByClass(Driver.class);
		job.setMapperClass(MapperTask.class);
		job.setReducerClass(ReducerTask.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


class MapperTask extends Mapper <LongWritable, Text, Text , IntWritable>
{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		Text test1 = new Text();
		IntWritable intWritable1 = new IntWritable();
		
		String token=value.toString();
		String[] tokens=token.split(",");
		
		if(tokens[0]!="#DATE") {
			
			//1. Date on which maximum number of accidents took place.
			String key1 = "Date_" + tokens[0];

			context.write(new Text(key1.toString()), new IntWritable(1));
			
			//2. Borough with maximum count of accident fatality
			String key2 = "Time_" + tokens[1];
			int personsFatality = 	Integer.parseInt(tokens[4])
								  + Integer.parseInt(tokens[6])
								  + Integer.parseInt(tokens[8])
								  + Integer.parseInt(tokens[10]);

			context.write(new Text(key2), new IntWritable(personsFatality));
			
			//3. Zip with maximum count of accident fatality
			String key3 = "Zip_" + tokens[2];
			int zipPersonsFatality = 	Integer.parseInt(tokens[4])
									  + Integer.parseInt(tokens[6])
									  + Integer.parseInt(tokens[8])
									  + Integer.parseInt(tokens[10]);
			context.write(new Text(key3), new IntWritable(zipPersonsFatality));
			
			//4. Which vehicle type is involved in maximum accidents
			String key4 = "VechileType_" + tokens[11];

			context.write(new Text(key4), new IntWritable(1));
			
			String[] year = tokens[0].split("/");
			
			//5. Year in which maximum Number Of Persons and Pedestrians Injured
			String key5 = "YearPpInjured_" + year[2];
			int peoplePedestriansInjured = 	  Integer.parseInt(tokens[3])
											+ Integer.parseInt(tokens[5]);
			context.write(new Text(key5), new IntWritable(peoplePedestriansInjured));
			
			//6. Year in which maximum Number Of Persons and Pedestrians Killed
			String key6 = "YearPpKilled_" + year[2];
			int peoplePedestriansKilled = 	  Integer.parseInt(tokens[4])
											+ Integer.parseInt(tokens[6]);
			context.write(new Text(key6), new IntWritable(peoplePedestriansKilled));
			
			//7. Year in which maximum Number Of Cyclist Injured and Killed (combined)
			String key7 = "cyclistInjuredKilled_" + year[2];
			int cyclistInjuredKilled = 	  	  Integer.parseInt(tokens[7])
											+ Integer.parseInt(tokens[8]);
			context.write(new Text(key7), new IntWritable(cyclistInjuredKilled));
			
			//8. Year in which maximum Number Of Motorist Injured and Killed (combined)
			String key8 = "motoristsInjuredKilled_" + year[2];
			int motoristsInjuredKilled = 	  Integer.parseInt(tokens[9])
											+ Integer.parseInt(tokens[10]);
			context.write(new Text(key8), new IntWritable(motoristsInjuredKilled));
			

		}
	
	}
		
}


class ReducerTask extends Reducer <Text, IntWritable, Text, IntWritable>
{
	String Date = new String();
	int DateMaxAccidents = Integer.MIN_VALUE;
	
	String Borough  = new String();
	int BoroughMaxAccidents = Integer.MIN_VALUE;
	
	String Zip  = new String();
	int ZipMaxAccidents = Integer.MIN_VALUE;
	
	String VehicleType = new String();
	int VehicleTypeMaxAccidents = Integer.MIN_VALUE;
	
	String YearPpInured = new String();
	int YearPpInuredMaxAccidents = Integer.MIN_VALUE;
	
	String YearPpKilled = new String();
	int YearPpKilledMaxAccidents = Integer.MIN_VALUE;
	
	String cyclist = new String();
	int cyclistMaxAccidents = Integer.MIN_VALUE;
	
	String motorists = new String();
	int motoristsMaxAccidents = Integer.MIN_VALUE;
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		
		IntWritable intWritable1   = new IntWritable();
		Text text1 = new Text();
		int sumVal = 0;
		
		for (IntWritable val : values)
		{
			sumVal+=val.get();	
		}
		
		if(key.toString().startsWith("Date_")) {
			if(sumVal>DateMaxAccidents) {
				Date = key.toString();
				DateMaxAccidents=sumVal;
			}
		}
		else if(key.toString().startsWith("Time_")) {
			if(sumVal>BoroughMaxAccidents) {
				Borough = key.toString();
				BoroughMaxAccidents=sumVal;
			}
		}
		else if(key.toString().startsWith("Zip_")) {
			if(sumVal>ZipMaxAccidents) {
				Zip = key.toString();
				ZipMaxAccidents=sumVal;
			}
		}
		else if(key.toString().startsWith("VechileType_")) {
			if(sumVal>VehicleTypeMaxAccidents) {
				VehicleType = key.toString();
				VehicleTypeMaxAccidents=sumVal;
			}
		}
		else if(key.toString().startsWith("YearPpInjured_")) {
			if(sumVal>YearPpInuredMaxAccidents) {
				YearPpInured = key.toString();
				YearPpInuredMaxAccidents=sumVal;
			}
		}
		else if(key.toString().startsWith("YearPpKilled_")) {
			if(sumVal>YearPpKilledMaxAccidents) {
				YearPpKilled = key.toString();
				YearPpKilledMaxAccidents=sumVal;
			}
		}
		else if(key.toString().startsWith("cyclistInjuredKilled_")) {
			if(sumVal>cyclistMaxAccidents) {
				cyclist = key.toString();
				cyclistMaxAccidents=sumVal;
			}
		}
		else if(key.toString().startsWith("motoristsInjuredKilled_")) {
			if(sumVal>motoristsMaxAccidents) {
				motorists = key.toString();
				motoristsMaxAccidents=sumVal;
			}
		}
		
		
	 		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		context.write(new Text(Date), new IntWritable(DateMaxAccidents));
		
		context.write(new Text(Borough), new IntWritable(BoroughMaxAccidents));
		
		context.write(new Text(Zip), new IntWritable(ZipMaxAccidents));
		
		context.write(new Text(VehicleType), new IntWritable(VehicleTypeMaxAccidents));
		
		context.write(new Text(YearPpInured), new IntWritable(YearPpInuredMaxAccidents));
		
		context.write(new Text(YearPpKilled), new IntWritable(YearPpKilledMaxAccidents));
		
		context.write(new Text(cyclist), new IntWritable(cyclistMaxAccidents));
		
		context.write(new Text(motorists), new IntWritable(motoristsMaxAccidents));
		
	  }
}


