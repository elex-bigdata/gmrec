package com.elex.gmrec.algorithm;

import java.io.Serializable;

public class ItemPrefDTO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4927779034715010854L;
	
	
	private String dst_itemId;
	
	private String pref;
	

	public String getDst_itemId() {
		return dst_itemId;
	}

	public void setDst_itemId(String dst_itemId) {
		this.dst_itemId = dst_itemId;
	}

	public String getPref() {
		return pref;
	}

	public void setPref(String pref) {
		this.pref = pref;
	}
	
	
	public String toString(){
		return ","+dst_itemId+","+pref;
	}

}
