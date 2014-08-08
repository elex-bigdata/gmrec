package com.elex.gmrec.algorithm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;



public class TagSimilarityParse extends Configured implements Tool {

	public static class MyMapper extends
			Mapper<IntWritable, VectorWritable, Text, Text> {

		private Text nKey = new Text();
		private DecimalFormat df = new DecimalFormat("#.###");
		private Iterator<Vector.Element> nonZeroElements = null;
		private Vector.Element nonZeroElement = null;
		private Text item_pair_pref = new Text();

		public void map(IntWritable key, VectorWritable value, Context context)
				throws IOException, InterruptedException {
			
			nonZeroElements = value.get().iterateNonZero();
			while (nonZeroElements.hasNext()) {
				nonZeroElement = nonZeroElements.next();
				nKey.set(key.toString());
				item_pair_pref.set(nonZeroElement.index() + ","+ df.format(nonZeroElement.get()));
				context.write(nKey, item_pair_pref);
				nKey.set(nonZeroElement.index() + "");
				item_pair_pref.set(key + "," + df.format(nonZeroElement.get()));
				context.write(nKey, item_pair_pref);//因为输入目录只包含了相似度矩阵的上半部分，还需要输出下半部分
			}

		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private int topN;
		private double range;		
		private List<ItemPrefDTO> itemPairList = new ArrayList<ItemPrefDTO>();
		private String[] itempref;
		private int i;
		private int loop;
		private String dtoStr;
		private Map<String, String> id_index_map = new HashMap<String, String>();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			itemPairList.clear();
			StringBuilder sb = new StringBuilder(200);
			i = 0;
			for (Text val : values) {
				itempref = val.toString().split(",");
				if (Double.parseDouble(itempref[1]) > range) {
					ItemPrefDTO dto = new ItemPrefDTO();
					dto.setDst_itemId(itempref[0].trim());
					dto.setPref(itempref[1].trim());
					itemPairList.add(dto);
				}

			}

			Collections.sort(itemPairList, new ItemPrefComparator());

			if (topN > itemPairList.size()) {
				loop = itemPairList.size();
			} else {
				loop = topN;
			}

			sb.append(id_index_map.get(key.toString())).append("\t");
			while (i < loop) {
				sb.append(id_index_map.get(itemPairList.get(i).getDst_itemId()));
				if (i != loop-1) {
					sb.append(":");					
				}
				i++;
			}
			
			if(loop>0){
				context.write(null, new Text(sb.toString()));
			}
			

		}
				

		protected void setup(Context context) throws IOException,
				InterruptedException {			
					
			Configuration configuration = context.getConfiguration();
			topN = Integer.parseInt(configuration.get("topN"));
			range = Double.parseDouble(configuration.get("range")) / 100;

			FileSystem fs = FileSystem.get(configuration);
			FileStatus[] files = fs.listStatus(new Path(configuration.get("id_index_file")));
			SequenceFile.Reader reader = null;
			for (FileStatus file : files) {
				if (!file.isDirectory()) {
					Path hdfs_src = file.getPath();
					if (file.getPath().getName().contains("part-r")) {
						reader = new SequenceFile.Reader(configuration, Reader.file(hdfs_src));
						Writable key = (Writable) ReflectionUtils.newInstance(
								reader.getKeyClass(), configuration);
						Writable value = (Writable) ReflectionUtils
								.newInstance(reader.getValueClass(),
										configuration);
						while (reader.next(key, value)) {
							id_index_map.put(key.toString(), value.toString());
						}
					}
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TagSimilarityParse(),args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		conf.set("range", PropertiesUtils.getThreshold());
		conf.set("topN", PropertiesUtils.getTopN());
		conf.set("id_index_file", PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFTEMP + "/preparePreferenceMatrix/itemIDIndex");
		Job job = Job.getInstance(conf, "SimilarityParse");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setJarByClass(TagSimilarityParse.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		FileInputFormat.addInputPath(job, new Path(PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFTEMP+ "/pairwiseSimilarity"));
		String out = PropertiesUtils.getGmRecRootFolder()+Constants.TAGSIMOUT;
		HdfsUtils.delFile(fs, out);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(out));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static Map<String, List<String>> getTagSimMap() throws IOException {

		Map<String, List<String>> tagTopN = new HashMap<String, List<String>>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path tagSimPath = new Path(PropertiesUtils.getGmRecRootFolder()+ Constants.TAGSIMOUT);
		FileStatus[] files = fs.listStatus(tagSimPath);
		Path hdfs_src;
		for (FileStatus file : files) {
			if (!file.isDirectory()) {
				hdfs_src = file.getPath();
				if (file.getPath().getName().contains("part")) {
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(hdfs_src)));
					String line = reader.readLine();
					while (line != null) {
						String[] kv = line.split("\\s");
						List<String> list = new ArrayList<String>();
						String[] items = kv[1].split(":");
						for(String item:items){							
							list.add(item);
						}
						tagTopN.put(kv[0], list);
						line = reader.readLine();
					}
					reader.close();
				}

			}

		}
		return tagTopN;
	}
}
