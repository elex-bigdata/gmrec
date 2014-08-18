package com.elex.gmrec.algorithm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
		Map<String,Map<String,String>> gidTagMap;
		Map<String, List<String>> simTagMap;
		Map<String,Map<String,Double>> gidLangRation;
		
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			tagTopN = TagCF.getTagTopNMap();
			gidTagMap = TagLoader.getGidTagMap();
			simTagMap = TagSimilarityParse.getTagSimMap();
			gidLangRation = getGameLanguageRation();
		}

		
				
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] list;			
			Map<String, Double> gameMap;
			String lang;
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
					gameMap =  gidLangRation.get(list[1]);
					Iterator<String> ite = gameMap.keySet().iterator();
					while(ite.hasNext()){
						lang = ite.next();
						writeToReduce(list[1],lang,gameMap.get(lang),context);
					}
					
				}
				
			}
			
		}
		
		protected String[] getGameTags(String gmOfTags){
			
			Set<String> tagSet = new HashSet<String>();
			String[] tagId;
			if(gmOfTags != null){
				tagId = gmOfTags.split(":");
				for(String tag:tagId){
					tagSet.add(tag);
					if(simTagMap.get(tag) != null){
						tagSet.addAll(simTagMap.get(tag));
					}
					
				}
			}
			
			return tagSet.toArray(new String[tagSet.size()]);
			
		}
		
		public Map<String,Map<String,Double>> getGameLanguageRation() throws IOException{
			Map<String,Map<String,Double>> result = new HashMap<String,Map<String,Double>>();
			Map<String,Double> gameMap;
			Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        FileStatus[] files = fs.listStatus(new Path(PropertiesUtils.getRatingFolder()+Constants.MERGEFOLDER));
	        Path hdfs_src;
	        BufferedReader reader = null;
	        double all = 0D;
	        double lang = 0D;
	        for(FileStatus file:files){
	        	
	        	if(!file.isDirectory()){
	        		hdfs_src = file.getPath();
	        		if(file.getPath().getName().contains("part")){
	        			try {
	        	            reader = new BufferedReader(new InputStreamReader(fs.open(hdfs_src)));      	                    	            
	        	            String line =reader.readLine();
	        	            while(line != null){
	        	            	String[] vList = line.split(",");
	        	            	gameMap = result.get(vList[1])!=null?result.get(vList[1]):new HashMap<String,Double>();
	        	            	all = gameMap.get("all")==null?1:gameMap.get("all")+1;
	        	            	gameMap.put("all", all);
	        	            	lang = gameMap.get(vList[3])==null?1:gameMap.get(vList[3])+1;
	        	            	gameMap.put(vList[3], lang);
	        	            	result.put(vList[1], gameMap);
	        	            	line = reader.readLine();
	        	            }
	        	           reader.close();
	        	        } finally {
	        	            IOUtils.closeStream(reader);
	        	        }
	        			
	        		}
	        	}
	        } 
	        
	        String gid;
	        Entry<String, Map<String, Double>> entry;
	        Iterator<Entry<String, Map<String, Double>>> ite = result.entrySet().iterator();
	        String language;
	        while(ite.hasNext()){
	        	entry =  ite.next();
	        	gid = entry.getKey();
	        	gameMap = entry.getValue();
	        	Iterator<String> gmIte = gameMap.keySet().iterator();
	        	while(gmIte.hasNext()){
	        		language = gmIte.next();
	        		if(!"all".equals(language)){
	        			gameMap.put(language, gameMap.get(language)/gameMap.get("all"));
	        		}
	        		
	        	}
	        	gameMap.remove("all");
	        	result.put(gid, gameMap);
	        }
	        
			
			return result;
			
		}
		
		
		protected void writeToReduce(String gid,String lang,Double ratio,Context context) throws IOException, InterruptedException{
			String tags;
			String[] tagId;
			List<String> topN = new ArrayList<String>();
			StringBuffer rec = new StringBuffer(200);
			Set<String> set = new HashSet<String>();
			Map<String, String> gameTagMap;
			gameTagMap = gidTagMap.get(gid);
			
			if(gameTagMap != null){
				tags = gameTagMap.get(lang)!=null?gameTagMap.get(lang):TagLoader.getTag(gameTagMap);				
			}else{
				tags = Constants.DEFAULTTAG;
			}
			
			if(tags != null){
				tagId = getGameTags(tags);
				for(String tag:tagId){
					if(tagTopN.get(tag)!=null){
						set.addAll(tagTopN.get(tag));
					}																				
				}
			}
															
			topN.addAll(set);
			int size = Integer.parseInt(PropertiesUtils.getItemRecNumber());
			ratio = ratio>1?1:ratio;
			
			size = new Double(size*ratio).intValue();
			
			if(size > 0 && topN.size() > 0){
				if(topN.size()<size){
					for(String game:topN){							
						rec.append(game+",");
					}
					context.write(new Text(gid), new Text("02_"+ rec.substring(0, rec.toString().length()-1)));
				}else{
					topN = RandomUtils.randomTopN(size, topN);
					for(String game:topN){							
						rec.append(game+",");
					}
					context.write(new Text(gid), new Text("02_"+ rec.substring(0, rec.toString().length()-1)));
				}
			}
			
		}
			
	}
	
	
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
				
		Set<String> unionRec = new HashSet<String>();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int size = Integer.parseInt(PropertiesUtils.getItemRecNumber());
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
							unionRec.add(rec);
						}
						
					}
				}
			} catch (JSONException e) {				
				e.printStackTrace();
			}
			
			topN.addAll(unionRec);
			
			if(topN.size()>size){
				recSet.addAll(RandomUtils.randomTopN(size,topN));
			}else{
				recSet.addAll(topN);
			}
			
			if (recSet.size() > 0) {
				Iterator<String> ite = recSet.iterator();
				StringBuffer sb = new StringBuffer(200);
				sb.append(key.toString() + "\t");
				sb.append("[");
				int i = 0;
				while(ite.hasNext() && i<size){
					sb.append("{");
					sb.append("\"" + ite.next() + "\":" + "0");
					sb.append("}");
					sb.append(",");
					i++;
				}
				
				context.write(null,new Text(sb.substring(0, sb.toString().length() - 1)+ "]"));
			}									
						
		}				
		
	}
}
