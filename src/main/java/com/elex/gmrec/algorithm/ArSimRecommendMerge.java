package com.elex.gmrec.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;
import com.elex.gmrec.comm.RandomUtils;
import com.elex.gmrec.etl.TagLoader;

public class ArSimRecommendMerge extends Configured implements Tool {

	/**
	 * 功能：将ARRec和simRec的数据去重合并，对没有推荐结果的gid，随机取该gid对应tags的topNgid的size作为该gid的相似推荐结果
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ArSimRecommendMerge(), args);
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"ArSimRecommendMerge");
		job.setJarByClass(ArSimRecommendMerge.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		Path itemCfRecParse = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.CFSIMOUTPUT);
		FileInputFormat.addInputPath(job, itemCfRecParse);	
		
		Path tagCfRecFinal = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.ARULEOUTPUT);
		FileInputFormat.addInputPath(job, tagCfRecFinal);
		
		Path gidFile = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.GIDMAPPINGFILE);
		FileInputFormat.addInputPath(job, gidFile);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		Path output = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.ARSIMTAGRECMERGE);
		HdfsUtils.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		Map<String,List<String>> tagTopN;
		Map<String,String> gidTagMap;
		
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			tagTopN = TagCF.getTagTopNMap();
			gidTagMap = TagLoader.getGidTagMap();
		}

		
				
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] list;
			String tags;
			String[] tagId;
			List<String> topN;
			StringBuffer rec = new StringBuffer(200);
			
			String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(pathName.contains(Constants.CFSIMOUTPUT)){
				list = value.toString().split("\\s");
				context.write(new Text(list[0]), new Text("01_"+list[1]));
			}else if(pathName.contains(Constants.ARULEOUTPUT)){
				list = value.toString().split("\\s");
				context.write(new Text(list[0]), new Text("01_"+list[1]));
			}else if(pathName.contains(Constants.GIDMAPPINGFILE)){
				list = value.toString().split(",");
				if(list.length == 2){
					tags = gidTagMap.get(list[1]);
					if(tags != null){
						tagId = tags.split(":");
						for(String tag:tagId){
							topN = tagTopN.get(tag);
							if(topN.size()>0){
								for(String gid:topN){
									rec.append(gid+",");
								}
								context.write(new Text(list[1]), new Text("02_"+ rec.subSequence(0, rec.toString().length()-1)));
							}
							
						}
					}
				}
				
			}
			
		}
			
	}
	
	
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
				
		Set<String> unionRec = new HashSet<String>();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int size = Integer.parseInt(PropertiesUtils.getItemRecNumber());
			int needTopN;
			String gid,recStr;
			String[] gids;
			unionRec.clear();
			List<String> topN = new ArrayList<String>();
			Set<String> recSet = new HashSet<String>();
			
			try {
				for (Text line : values) {
					if (line.toString().startsWith("01_")) {
						JSONArray json= new JSONArray(line.toString().substring(3,line.toString().length()));
						for (int i = 0; i < json.length(); i++) {
							JSONObject obj = (JSONObject) json.get(i);
							gid = obj.keys().next().toString();
							unionRec.add(gid);							
						}
					}else if(line.toString().startsWith("02_")){
						recStr = line.toString().substring(3,line.toString().length());
						gids = recStr.split(",");
						for(String rec:gids){
							recSet.add(rec);
						}
						
					}
				}
			} catch (JSONException e) {				
				e.printStackTrace();
			}
			
			topN.addAll(recSet);
			
			if(unionRec.size()<size){
				needTopN = size - unionRec.size();
				if(needTopN > 0){
					unionRec.addAll(RandomUtils.randomTopN(needTopN,topN));
				}
			}
			
			if (unionRec.size() > 0) {
				Iterator<String> ite = unionRec.iterator();
				StringBuffer sb = new StringBuffer(200);
				sb.append(key.toString() + "\t");
				sb.append("[");
				int i = 0;
				while(ite.hasNext() && i<size){
					sb.append("{");
					sb.append("\"" + ite.next() + "\":" + "\"0\"");
					sb.append("}");
					sb.append(",");
					i++;
				}
				
				context.write(null,new Text(sb.substring(0, sb.toString().length() - 1)+ "]"));
			}									
						
		}				
		
	}
}
