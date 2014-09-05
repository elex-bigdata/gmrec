package com.elex.gmrec.algorithm;

import java.io.IOException;

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

import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.etl.IDMapping;


public class CfRecParse extends Configured implements Tool {

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		String[] uidMap;
		String uid;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			uidMap = IDMapping.getUidIntStrMap();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			 String[] kv = value.toString().split("\\s");
			 if(kv != null){
				 if(kv.length==2){
					 uid = uidMap[Integer.parseInt(kv[0].trim())];
					 context.write(new Text(uid), new Text(kv[1]));
				 }
			 }
			 		 
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		

		String[] gidMap;
		String itemStr;
		String[] itemArr;
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuffer sb = new StringBuffer(200);
			
			for(Text v:values){
				itemStr = v.toString().trim().replace("[", "").replace("]", "");
				itemArr = itemStr.split(",");				
				sb.append(key.toString()+"\t");
				sb.append("[");
				for (int i = 0; i < itemArr.length; i++) {
					String[] item = itemArr[i].split(":");
					sb.append("{");
					String gid = gidMap[Integer.parseInt(item[0])];
					sb.append("\""+gid+"\":"+item[1]);
					sb.append("}");
					if(i!=itemArr.length-1){
						sb.append(",");
					}
					
				}
				sb.append("]\r\n");
			}
			context.write(null, new Text(sb.toString()));
		}
				

		protected void setup(Context context) throws IOException,
				InterruptedException {			
			gidMap = IDMapping.getGidIntStrMap();
		}

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new CfRecParse(),args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "CfRecParse");
		job.setInputFormatClass(TextInputFormat.class);
		job.setJarByClass(CfRecParse.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		HdfsUtils.delFile(fs, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
