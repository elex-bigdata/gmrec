package com.elex.gmrec.analyze;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector.Element;


public class SequenceFileReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		DecimalFormat df = new DecimalFormat("0.000000");
		   String uri=args[0];
		   Configuration conf=new Configuration();
		   FileSystem fs=FileSystem.get(URI.create(uri),conf);
		   Path path=new Path(uri);
		   SequenceFile.Reader reader=null;
		   File dstFile = new File(args[1]);
	       BufferedWriter out = new BufferedWriter(new FileWriter(dstFile));	
		   try{
		    reader=new SequenceFile.Reader(conf,Reader.file(path));
		    //IntWritable key=(IntWritable)ReflectionUtils.newInstance(reader.getKeyClass(),conf);
		    //WeightedVectorWritable value=(WeightedVectorWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		    Writable key=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(),conf);
		    Writable value=(Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		    //long position=reader.getPosition();
		    StringBuffer  str = new StringBuffer(1000);
		    while(reader.next(key,value)){
		     //String syncSeen=reader.syncSeen()?"*":"#";
		    out.write(key+","+value+"\r\n");		    			    	
		    	//stem.out.println(key+","+value);
		     //position=reader.getPosition();
		   /* Iterator<Element> it = value.get().iterator();
			str.delete(0, str.toString().length());
			str.append("{");
			while (it.hasNext()) {
				Element e = it.next();
				str.append(e.index() + ":").append(df.format(e.get())).append(",");
			}
			out.write(key + ","+ str.substring(0, str.toString().length() - 1) + "}"+ "\r\n");*/
		    }
		    out.close();
		   }finally{
		    IOUtils.closeStream(reader);
		   }


	}
	
	public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new Text(value);
	}

}
