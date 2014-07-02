package com.elex.gmrec.etl;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.ParseUtils;
import com.elex.gmrec.comm.PropertiesUtils;
import com.elex.gmrec.comm.StrLineParseTool;

public class PrepareInputForCF  implements StrLineParseTool{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		prepareInput();
	}
	
	
	public static void prepareInput() throws Exception{
		 String input = PropertiesUtils.getRatingFolder()+Constants.MERGEFOLDER;
		 String output = PropertiesUtils.getGmRecRootFolder()+Constants.CFINPUT;
		 ParseUtils.parseTextOutput(input, output, new PrepareInputForCF());
	}


	@Override
	public String parse(String line) throws Exception {
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
		Path uidMappingFile = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.UIDMAPPINGFILE);
		Path gidMappingFile = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.GIDMAPPINGFILE);
		Map<String,Integer> uidMap = ParseUtils.readIdMapFile(fs,uidMappingFile);
	    Map<String,Integer> gidMap = ParseUtils.readIdMapFile(fs,gidMappingFile);
		
		String[] vList = line.split(",");
    	if(vList.length==3){
        	return new String(Integer.toString(uidMap.get(vList[0]))+","+Integer.toString(gidMap.get(vList[1]))+","+vList[2]+"\r\n");
    	}
		return null;
	}

}
