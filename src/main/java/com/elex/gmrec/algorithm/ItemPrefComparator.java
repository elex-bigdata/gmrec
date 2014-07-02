package com.elex.gmrec.algorithm;

import java.util.Comparator;

public class ItemPrefComparator implements Comparator<ItemPrefDTO> {

	@Override
	public int compare(ItemPrefDTO arg0, ItemPrefDTO arg1) {
		double pref1 = Double.parseDouble(arg0.getPref());
		double pref2 = Double.parseDouble(arg1.getPref());
		if(pref1>pref2){
			return -1;
		}else if(pref1<pref2){
			return 1;
		}else{
			return 0;
		}
		
	}

}
