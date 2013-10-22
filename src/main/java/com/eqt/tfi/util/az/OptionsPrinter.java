package com.eqt.tfi.util.az;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This is just here to learn what Azkaban is doing.
 * spits out all the arguments and vars it receives.
 * @author gman
 *
 */
public class OptionsPrinter {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(args != null) {
			System.out.println();
			System.out.println("Origional Arguments:");
			for(String arg : args)
				System.out.println(arg);
		}

		GenericOptionsParser p = new GenericOptionsParser(args);
		Configuration conf = p.getConfiguration();
		
		if(p.getRemainingArgs() != null) {
			System.out.println();
			System.out.println("#########################################################");
			System.out.println("args not picked up by a hadoop conf:");
			for(String arg : p.getRemainingArgs())
				System.out.println(arg);
		}
		
		Map<String, String> getenv = System.getenv();
		System.out.println();
		System.out.println("#########################################################");
		System.out.println("Current system enviroment variables:");
		for(String key : getenv.keySet())
			System.out.println(key + "="+getenv.get(key));

		System.out.println();
		System.out.println("#########################################################");
		System.out.println("Hadoop Configuration:");
		for(Iterator<Entry<String, String>> it = conf.iterator();it.hasNext();) {
			Entry<String, String> next = it.next();
			System.out.println(next.getKey() + ": " + next.getValue());
		}
		
	}

}
