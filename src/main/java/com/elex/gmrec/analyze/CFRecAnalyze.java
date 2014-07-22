package com.elex.gmrec.analyze;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CFRecAnalyze {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		File file = new File(args[0]);

		process(file,args[1]);
	}

	private static void process(File file,String dist) throws IOException {

		BufferedReader in;
		BufferedWriter out = new BufferedWriter(new FileWriter(new File(dist)));
		in = new BufferedReader(new FileReader(file));
		String line = in.readLine();
		while(line != null){			
			String[] nLine = line.replaceAll("[\"|\\[|\\]|\\{|\\}]", "").split("\t");
			String[] rec = nLine[1].split(",");
			for(String s:rec){
				String[] kv = s.split(":");
				out.write(nLine[0]+","+kv[0]+","+kv[1]+"\r\n");				
			}
			line = in.readLine();
		}
		
		in.close();
		out.close();

	}

}
