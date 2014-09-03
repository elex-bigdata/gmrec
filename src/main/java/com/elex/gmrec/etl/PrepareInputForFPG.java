package com.elex.gmrec.etl;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;

public class PrepareInputForFPG extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PrepareInputForFPG(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"prepareInputForFPG");
		job.setJarByClass(PrepareInputForFPG.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		Path in = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.CFINPUT);
		FileInputFormat.addInputPath(job, in);		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path output = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.FPGINPUT);
		HdfsUtils.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		Set<String> miniGame;
		
		 @Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			 miniGame = FilterUtils.getMiniGM();
		}
		String[] vList;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			vList = value.toString().split(",");
			if(vList.length==3){
				if(miniGame.contains(vList[1])){
					if(!vList[2].equals("0")){
						context.write(new Text(vList[0]), new Text(vList[1]));
					}
				}				
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
				
		@Override
		protected void reduce(Text key, Iterable<Text> gidList,Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer(200);
			for(Text gid:gidList){
				sb.append(gid).append(" ");
			}
			context.write(null, new Text(sb.toString().trim()));
		}		
	}
}
