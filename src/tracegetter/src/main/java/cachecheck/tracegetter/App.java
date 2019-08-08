package cachecheck.tracegetter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;

public class App 
{
    public static void main( String[] args ) throws Exception
    {
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
     * 打印进程输出
     *
     * @param process 进程
     */
    private static void readProcessOutput(final Process process) {
        // 将进程的正常输出在 System.out 中打印，进程的错误输出在 System.err 中打印
        read(process.getInputStream(), System.out);
        read(process.getErrorStream(), System.err);
    }

    // 读取输入流
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
