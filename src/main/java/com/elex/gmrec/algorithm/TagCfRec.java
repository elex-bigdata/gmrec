package com.elex.gmrec.algorithm;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;

public class TagCfRec extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TagCfRec(), args);
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"TagCfRec");
		job.setJarByClass(TagCfRec.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		Path rating = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.MERGEFOLDER);
		FileInputFormat.addInputPath(job, rating);	
		Path tacfout = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFRECPARSE);
		FileInputFormat.addInputPath(job, tacfout);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path output = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFRECFINAL);
		HdfsUtils.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		String[] list;
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(pathName.contains(Constants.MERGEFOLDER)){
				list = value.toString().split(",");
				context.write(new Text(list[0]), new Text("01_"+list[1]));
			}else if(pathName.contains(Constants.TAGCFRECPARSE)){
				list = value.toString().split("\\t");
				context.write(new Text(list[0]), new Text("02_"+list[1]));
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		Set<String> hasPlaySet = new HashSet<String>();
		Set<String> recSet = new HashSet<String>();
		Set<String> result = new HashSet<String>();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			hasPlaySet.clear();
			recSet.clear();
			for(Text line:values){
				if(line.toString().startsWith("01_")){
					hasPlaySet.add(line.toString().substring(3, line.toString().length()));					
				}else if(line.toString().startsWith("02_")){
					String[] list = line.toString().substring(3, line.toString().length()).split(",");
					for(String gid:list ){
						recSet.add(gid);						
					}					
				}				
			}
			
			result.clear();
			result.addAll(recSet);
			result.removeAll(hasPlaySet);
			
			Iterator<String> ite = result.iterator();
			int i=0;
			StringBuffer sb = new StringBuffer(200);
			sb.append(key.toString()+"\t");
			sb.append("[");
			while(ite.hasNext()){
				sb.append("{");			
				sb.append("\""+ite.next()+"\":"+"null");
				sb.append("}");
				i++;
				if(i!=result.size()-1){
					sb.append(",");
				}
			}
			
			sb.append("]\r\n");
			context.write(null,new Text(sb.toString()));
						
		}		
	}
}
