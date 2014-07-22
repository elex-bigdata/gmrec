package com.elex.gmrec.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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

public class TagRanking extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		prepareInput();
		ToolRunner.run(new Configuration(), new TagRanking(), args);
	}
	
	public static void prepareInput() throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		FileStatus[] files = fs.listStatus(new Path(PropertiesUtils.getGmRecRootFolder() + Constants.TAGCFIN),new PathFilter() {
		    @Override
		    public boolean accept(Path path) {
		      String name = path.getName();
		      return name.startsWith("hasgid");
		    }
		  });
		
		for (int i = 0; i < files.length; i++) {
			HdfsUtils.backupFile(fs,conf,files[i].getPath().toString(), PropertiesUtils.getGmRecRootFolder() + Constants.TAGRANK+files[i].getPath().getName());
			fs.delete(files[i].getPath(), true);			
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"TagRanking");
		job.setJarByClass(TagRanking.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		Path in = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.TAGRANK);
		FileInputFormat.addInputPath(job, in);		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path output = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.TAGRANKOUT);
		HdfsUtils.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		 String[] vList;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			vList = value.toString().split(",");
			if(vList.length==3){
				context.write(new Text(vList[1]), new Text(vList[2]));
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		Map<String,Integer> gcMap = new HashMap<String,Integer>();
		int count = 0;
		String gid;
		int size = PropertiesUtils.getTagRankTopN();
		Entry<String,Integer> entry;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			gcMap.clear();
			for(Text gid:values){
				count = gcMap.get(gid)!=null?gcMap.get(gid)+1:1;
				gcMap.put(gid.toString(), count);
			}
			List<Map.Entry<String,Integer>> list = new ArrayList<Map.Entry<String,Integer>>(gcMap.entrySet());
			
			Collections.sort(list,new Comparator<Map.Entry<String,Integer>>() {
	            //降序排列
				@Override
				public int compare(Entry<String, Integer> o1,
						Entry<String, Integer> o2) {
					return o2.getValue().compareTo(o1.getValue());
				}
	            
	        });
			
			size = list.size()>size?size:list.size();
			
			Iterator<Entry<String, Integer>> ite = list.subList(0, size).iterator();
			StringBuffer sb = new StringBuffer(200);
			
			while(ite.hasNext()){
				entry = ite.next();
				gid = entry.getKey();
				sb.append(gid).append(",");
				
			}
			context.write(new Text(key.toString()), new Text(sb.substring(0,sb.toString().length()-1)));
			
		}		
	}
}
