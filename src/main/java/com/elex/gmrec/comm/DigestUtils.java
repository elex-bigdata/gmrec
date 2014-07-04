package com.elex.gmrec.comm;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DigestUtils {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws NoSuchAlgorithmException 
	 */
	public static void main(String[] args) throws NoSuchAlgorithmException, IOException {
		File file = new File(args[0]);
		System.out.println(getDigest(file));
	}
	
	public static String getDigest(File file) throws IOException, NoSuchAlgorithmException{
						
		String returnValue = "";
	    try {
	      FileInputStream fis = new FileInputStream(file);
	      MessageDigest algorithm = MessageDigest.getInstance("MD5"); 
	      BufferedInputStream bis = new BufferedInputStream(fis);
	      DigestInputStream dis = new DigestInputStream(bis, algorithm);
	      int ch;
	      while ((ch = dis.read()) != -1);
	      dis.close();
	      StringBuffer hexString = new StringBuffer();
	      byte digest[] = algorithm.digest();
	      int digestLength = digest.length;
	      for (int i = 0; i < digestLength; i++) {
	        hexString.append(hexDigit(digest[i]));
	      }
	      returnValue = hexString.toString();
	    } catch (IOException e) {
	      System.err.println("Error generating digest for: " + file.getName());
	    }
	    return returnValue;
		
	}
	
	
	static private String hexDigit(byte x) {
	    StringBuffer sb = new StringBuffer();
	    char c;
	    c = (char) ((x >> 4) & 0xf);
	    if (c > 9) {
	      c = (char) ((c - 10) + 'a');
	    } else {
	      c = (char) (c + '0');
	    }
	    sb.append(c);
	    c = (char) (x & 0xf);
	    if (c > 9) {
	      c = (char) ((c - 10) + 'a');
	    } else {
	      c = (char) (c + '0');
	    }
	    sb.append(c);
	    return sb.toString();
	  }
	

}
