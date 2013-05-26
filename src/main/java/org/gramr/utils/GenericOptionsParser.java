package org.gramr.utils;

public class GenericOptionsParser {
	public static Integer optionExists(String[] args, String arg) {
		Integer pos = null;
		
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-" + arg)) {
				pos = i;
				break;
			}
		}
		
		return pos;
	}
	
	/*
	 * @param args - the input arguments
	 * @param arg - the argument value user is looking for
	 * @return argValue - the value if found, otherwise null
	 */
	public static String getOptionValue(String[] args, String arg) {
		Integer pos = optionExists(args, arg);
		if (pos == null || pos + 1 >= args.length)
			return null;
		
		return args[pos + 1];
	}
}
