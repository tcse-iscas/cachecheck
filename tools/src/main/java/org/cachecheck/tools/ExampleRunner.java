package org.cachecheck.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;

public class ExampleRunner 
{
    public static void main( String[] args ) throws Exception
    {
    	ArrayList<String> argList = new ArrayList<String>(Arrays.asList(args));
		String usage = "An example: java -jar tools-examplerunner.jar $ExampleList $SparkDir \r\n"
				+ "$ExampleList is the file defined which examples will be runned for the following CacheCheck detection. "
				+ "It is a .xml file located in the 'resource' directory.\r\n"
				+ "$SparkDir is the main directory of the Spark. The directory must contains a 'bin' dir with a 'run-example' script. \r\n";
		if(argList.contains("-h") || argList.contains("help")) {
			System.out.println("Welcome to use ExampleRunner! It is used for automatically running Spark's examples.\r\n"
					+ usage);
			System.exit(0);
		} else if(argList.size() != 2) {
			System.out.println("Wrong arguments! \r\n" + usage);
			System.exit(-1);
		}
    	String listPath = args[0];
    	String workspace = args[1];
        ExampleList list = new ExampleList(listPath);
        ArrayList<String> caseNames = list.getList();
        File dir = null;
        if(workspace != null) {
        	dir = new File(workspace);
        }
        for(String app: caseNames) {
        	System.out.println("========== Begin to run example "+ app + "==========");
        	String command = "bin/run-example " + app;
        	System.out.println("RUN: " + command);
        	Process process = Runtime.getRuntime().exec(command, null, dir);
        	readProcessOutput(process);
        	int status = process.waitFor();
        	if(status != 0) {
        		System.err.println("Failed to run example: " + app);
        		System.exit(-1);
        	}
        	System.out.println("========== End example "+ app + "==========");
        }
    }
    
    /**
     * Print process output
     *
     * @param process
     */
    private static void readProcessOutput(final Process process) {
        read(process.getInputStream(), System.out);
        read(process.getErrorStream(), System.err);
    }

    /**
     * Read the input stream
     * @param inputStream
     * @param out
     */
    private static void read(InputStream inputStream, PrintStream out) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                out.println(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
