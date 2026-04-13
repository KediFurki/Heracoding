package comapp;

import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Hashtable<Integer, String> h = new Hashtable<Integer, String>();
		h.put(123, "aaa");
		h.put(456, "bbbb");
		h.put(4567, "cc");
		String a = " 123 + [123] + [456] + [4567] + [123]";
		Pattern MY_PATTERN = Pattern.compile("\\[(.*?)\\]");
		Matcher m = MY_PATTERN.matcher(a);
		while (m.find()) {
		    int s =Integer.parseInt(m.group(1));
		    String res = h.get(s);
		    System.out.println(s );
		    a =a.replace("["+s+"]", res);
		    System.out.println(a);
		}  System.out.println("-->"+a);
		//result = new DoubleEvaluator().evaluate(operation);
	}

}
