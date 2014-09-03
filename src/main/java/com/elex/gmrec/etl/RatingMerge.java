package com.elex.gmrec.etl;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
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
import com.elex.gmrec.comm.Language;
import com.elex.gmrec.comm.PropertiesUtils;

public class RatingMerge extends Configured implements Tool  {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RatingMerge(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path[] inputDirs = getMergeInputFolders(fs);
		Job job = Job.getInstance(conf,"ratingMergeETL");
		job.setJarByClass(RatingMerge.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		for(Path in:inputDirs){
			FileInputFormat.addInputPath(job, in);
		}
		
		job.setOutputFormatClass(TextOutputFormat.class);
		String output = PropertiesUtils.getRatingFolder()+Constants.MERGEFOLDER;
		com.elex.gmrec.comm.HdfsUtils.delFile(fs, output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public Path[] getMergeInputFolders(FileSystem fs) throws IOException{
		int days = PropertiesUtils.getMergeDays();
		long now = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Set<Path> daySet = new HashSet<Path>();
		Path initPath = new Path(PropertiesUtils.getRatingFolder()+Constants.INITFOLDER);
		for(int i=0;i<days;i++){
			now = now- Long.valueOf(24L*60L*60L*1000L);
			String day = sdf.format(new Date(now));
			Path path = new Path(PropertiesUtils.getRatingFolder()+"/"+day);
			if(fs.exists(path)){
				daySet.add(path);
			}
			
		}
		if(daySet.size()<days && fs.exists(initPath)){
			
			daySet.add(initPath);
		}
		return daySet.toArray(new Path[daySet.size()]);
	}

	//TextOutputFormat的输出文件key为long的字节偏移量
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
				
		String[] vList;
							
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			vList = value.toString().split(",");
			
			if(vList.length==5){
				if(!vList[2].equals("0")){
					context.write(new Text(vList[0]+","+vList[1]+","+vList[4]), new Text(vList[2]));
					
				}
			}else if(vList.length==4){
				if(!vList[2].equals("0")){
					context.write(new Text(vList[0]+","+vList[1]+","+"en"), new Text(vList[2]));
				}
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		DecimalFormat df = new DecimalFormat("#.###");
		String lang;
		String[] ugl;
		
		@Override
		protected void reduce(Text key, Iterable<Text> rateList,Context context)
				throws IOException, InterruptedException {
			ugl = key.toString().split(",");
			lang = ugl.length==3?ugl[2]:"pt";
			lang = getLang(lang);
			
			double rate = 0;
			for(Text r:rateList){
				rate = Double.valueOf(r.toString())>rate?Double.valueOf(r.toString()):rate;
			}
			context.write(null, new Text(key.toString().substring(0, key.toString().lastIndexOf(","))+","+df.format(rate)+","+lang));
		}
		
		protected String getLang(String lang){
			Language[] ls = Language.values();
			for(Language l : ls){
				if(lang.equals(l.name())){
					return lang;
				}
			}
			
			return "pt";
		}
	}
}
