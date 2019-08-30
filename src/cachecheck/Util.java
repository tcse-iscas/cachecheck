package cachecheck;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Util {
	ArrayList<DAG> jobs;
	private ArrayList<Integer> rddshouldpersit;
	private ArrayList<String> actualSequence;
	private ArrayList<String> correctSequence;
	private String appName = "default";
	
	public Util(String traceFilePath, String jobFilePath, String appName) {
		jobs = new ArrayList<DAG>();
		rddshouldpersit = new ArrayList<Integer>();
		actualSequence = new ArrayList<String>();
		correctSequence = new ArrayList<String>();
		this.appName = appName;
		// Read jobfile
		File jobFile = new File(jobFilePath);
		System.out.println("Job file: " + jobFilePath);
		try {
			FileInputStream fisJob = new FileInputStream(jobFile);
			BufferedReader brJob = new BufferedReader(new InputStreamReader(fisJob));
			String jobString;
			while((jobString = brJob.readLine()) != null) {
				if(!jobString.equals(""))
					jobs.add(jobAsDAG(jobString));
			}
			brJob.close();
			fisJob.close();
		} catch (IOException e) {
			System.out.println("[ERROR] .job file error!");
			System.out.println(jobFile.getAbsolutePath());
			e.printStackTrace();
			System.exit(0);
		}
		// generate job dag objects
		for(int i = 0; i < jobs.size(); i++) {
			for(int j = i+1; j < jobs.size(); j++) {
				DAG job2 = jobs.get(j);
				ArrayList<Integer> branchPoints = jobs.get(i).getLargestBranchPoint(job2);
				rddshouldpersit.addAll(branchPoints);
			}
		}
		HashSet<Integer> set = new HashSet<Integer>(rddshouldpersit);
		rddshouldpersit = new ArrayList<Integer>(set);
		
		//read tracefile
		File traceFile = new File(traceFilePath);
		System.out.println("Trace file: " + traceFilePath);
		try {
			FileInputStream fisTrace = new FileInputStream(traceFile);
			BufferedReader brTrace = new BufferedReader(new InputStreamReader(fisTrace));
			String traceString;
			while((traceString = brTrace.readLine())!=null) {
				actualSequence.add(traceString);
			}
			brTrace.close();
			fisTrace.close();
		} catch (IOException e) {
			System.out.println("[ERROR] .trace file error!");
			System.out.println(traceFile.getAbsolutePath());
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private DAG jobAsDAG(String job) {
		String[] edges = job.split(",");
		String youngest = edges[0].split("-")[1];
		Vertex youngestV = new Vertex(Integer.parseInt(youngest));
		DAG dag = new DAG(youngestV);
		for(int i=1; i<edges.length;i++) {
			String[] rddPairs = edges[i].split("-");
			dag.addEdge(Integer.parseInt(rddPairs[1]), Integer.parseInt(rddPairs[0]));
		}
		return dag;
	}
	
	public ArrayList<Integer> getShouldPersistRDDs() {
		return rddshouldpersit;
	}
	
	public ArrayList<String> getActualSequence() {
		return actualSequence;
	}
	
	public ArrayList<String> generateCorrectSequence() throws Exception {
		ArrayList<String> result = new ArrayList<String>();
		ArrayList<Integer> rddFinishPersist = new ArrayList<Integer>();
		// Key for rddid, value for id of job which uses the rdd.
		HashMap<Integer, Integer> useCondition = new HashMap<Integer, Integer>();
		int jobNum = jobs.size();
		// generate the correct persist positions
		for(int i=0; i<jobNum; i++) {
			DAG job = jobs.get(i);
			for(Integer rddsp: rddshouldpersit) {
				if(!rddFinishPersist.contains(rddsp) && job.hasVertex(rddsp)) {
					result.add("persist "+ rddsp);
					rddFinishPersist.add(rddsp);
					useCondition.put(rddsp, i);
				}
			}
			result.add("job "+i);
		}
		// generate the correct unpersist positions
		for(Integer rddsp: rddshouldpersit) {
			int firstUse = useCondition.get(rddsp);
			int lastUse = firstUse;
			ArrayList<Integer> childrenSPrdd = new ArrayList<Integer>();
			for(int i=firstUse; i<jobNum; i++) {
				DAG job = jobs.get(i);
				if(job.hasVertex(rddsp)) {
					@SuppressWarnings("unchecked")
					ArrayList<Integer> otherRDDsp = (ArrayList<Integer>) rddshouldpersit.clone();
					otherRDDsp.remove(rddsp);
					boolean thisUse = job.isUsed(otherRDDsp, childrenSPrdd, rddsp);
					if(thisUse)
						lastUse = i;
				}
			}
				result.add(result.indexOf("job "+ lastUse)+1, "unpersist "+rddsp);
		}
		correctSequence = result;
		return result;
	}
	
	private boolean isLastJob(int jobId, ArrayList<String> sequence) {
		boolean result = false;
		for(String event : sequence) {
			if(event.equals("job "+jobId))
				result = true;
			else if(result)
				return false;
		}
		return result;
	}
	
	public ArrayList<Bug> detectBugs() throws Exception{
		ArrayList<Bug> report = new ArrayList<Bug>();
		if(correctSequence.size()==0) {
			System.out.println("Generate correct sequence first please!");
			return report;
		}
		int lastJob = -1;
		int nextJob = 0;
		for(String event: correctSequence) {
			String[] array = event.split(" ");
			String type = array[0];
			int id = Integer.parseInt(array[1]);
			switch (type) {
			case "persist":
				if(!actualSequence.contains("persist "+id))
					report.add(new Bug(id, Pattern.MissingPerssit, ""));
				else {
					int actualPos = actualSequence.indexOf("persist "+id);
					int nextJobPos = actualSequence.indexOf("job " + nextJob);
					// If persist is after the next job, it is a lagging persist.
					// nextJobPos can be -1, which means it is the last job.
					if (actualPos > nextJobPos)
						report.add(new Bug(id, Pattern.LaggingPersist, ""));
				}
				break;
			case "unpersist":
				// If the rdd is a no persist bug, no unpersist should not be a bug
				if((!actualSequence.contains("unpersist "+id)) && actualSequence.contains("persist "+id)) {
					// If the correct unpersist is after the last job, it is not a bug 
					if(!isLastJob(lastJob, actualSequence)) 
						report.add(new Bug(id, Pattern.MissingUnpersist, ""));
				} else if(actualSequence.contains("unpersist "+id)) {
					int actualPos = actualSequence.indexOf("unpersist "+id);
					int lastJobPos = actualSequence.indexOf("job " + lastJob);
					int nextJobPos = actualSequence.indexOf("job " + nextJob);
					// If unpersist is after the next job, it is a lagging unpersist.
					// The next job can be non-existent (nextJobPos == -1),
					// because it is after the last job. This situation is not a bug.
					if(actualPos > nextJobPos && nextJobPos > 0)
						report.add(new Bug(id, Pattern.LaggingUnpersist, ""));
					// If unpersist is before before the last job, 
					// it is a premature unpersist
					else if(actualPos < lastJobPos)
						report.add(new Bug(id, Pattern.PrematureUnpersist, ""));
				}
				break;
			case "job":
				lastJob = id;
				nextJob = id + 1;
			default:
				break;
			}
		}
		for(String event: actualSequence) {
			String[] array = event.split(" ");
			String type = array[0];
			int id = Integer.parseInt(array[1]);
			if (type.equals("persist") && !rddshouldpersit.contains(id)) {
				report.add(new Bug(id, Pattern.UnnecessaryPersist, ""));
			}
		}
		return report;
	}
	
	public void printDetectionReport(ArrayList<Bug> report) throws Exception{
		if(null == report) {
			System.out.println("Run detection process first please!");
			return;
		}
		for(Bug bug: report) {
			int id = bug.rddID;
			System.out.println("Bug: [" + bug.pattern + "] for RDD " + id);
		}
	}
	
	public void saveReport(String path, ArrayList<Bug> report) throws IOException {
		if(null == report) {
			System.out.println("Run detection process first please!");
			return;
		}
		File savePath = new File(path);
		if(savePath.isDirectory()) {
			path += appName+".report";
			File reportFile = new File(path);
			if(reportFile.exists()) {
				reportFile.delete();
			}
			reportFile.createNewFile();
			FileOutputStream fos = new FileOutputStream(reportFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos);
			for(Bug bug: report) {
				int id = bug.rddID;
				osw.write("Bug: [" + bug.pattern + "] for RDD " + id + "\r\n");
			}
			osw.close();
			fos.close();
		} else {
			System.out.println("Path must be the directory!");
		}
	}
	
	public ArrayList<Bug> deduplication(String perposFile, String rddinfoFile, ArrayList<Bug> report) throws Exception {
		ArrayList<Bug> result = new ArrayList<Bug>();
		Map<Integer, String> perpos = new HashMap<Integer, String>();
		Map<Integer, String> unperpos = new HashMap<Integer, String>();
		Map<Integer, String> rddinfo = new HashMap<Integer, String>();
		// Read perposFile
		File perFile = new File(perposFile);
		System.out.println("Perpos file: " + perposFile);
		try {
			FileInputStream fisPer = new FileInputStream(perFile);
			BufferedReader brPer = new BufferedReader(new InputStreamReader(fisPer));
			String perString;
			while((perString = brPer.readLine())!=null) {
				String[] pers = perString.split(" ");
				int persLength = pers.length;
				if(persLength <= 1)
					continue;
				String poString = "";
				for(int i = 2; i<persLength; i+=3) {
					poString+=pers[i];
				}
				if(pers[0].equals("persist"))
					perpos.put(Integer.parseInt(pers[1]), poString);
				else if (pers[0].equals("unpersist"))
					unperpos.put(Integer.parseInt(pers[1]), poString);
			}
			brPer.close();
			fisPer.close();
		} catch (IOException e) {
			System.out.println("[ERROR] .perpos file error!");
			e.printStackTrace();
			System.exit(0);
		}
		// Read rddinfoFile
		File infoFile = new File(rddinfoFile);
		System.out.println("RDDInfo file: " + rddinfoFile);
		try {
			FileInputStream fisInfo = new FileInputStream(infoFile);
			BufferedReader brInfo = new BufferedReader(new InputStreamReader(fisInfo));
			String rddString;
			brInfo.readLine();// The first line is the ID sequence of RDDs.
			while((rddString = brInfo.readLine())!=null) {
				String[] infos = rddString.split(" ");
				int infoLength = infos.length;
				if(infoLength <= 1)
					continue;
				String infoString = "";
				for(int i=4;i<infoLength;i+=3) {
					infoString+=infos[i];
				}
				int ID = Integer.parseInt(infos[0].substring(infos[0].indexOf("[")+1, infos[0].indexOf("]")));
				rddinfo.put(ID, infoString);
			}
			brInfo.close();
			fisInfo.close();
		} catch (Exception e) {
			System.out.println("[ERROR] .info file error!");
			e.printStackTrace();
			System.exit(0);
		}
		// Reverse bug reports
		for(Bug bug: report) {
			String location;
			switch (bug.pattern) {
			case MissingPerssit:
			case MissingUnpersist:
				location = rddinfo.get(bug.rddID);
				break;
			case LaggingPersist:
			case UnnecessaryPersist:
				location = perpos.get(bug.rddID);
				break;
			case PrematureUnpersist:
			case LaggingUnpersist:
				location = unperpos.get(bug.rddID);
				break;
			default:
				throw new Exception("Unknown pattern!");
			}
			bug.setLocation(location);
			boolean newBug = true;
			for(Bug anotherBug: result) {
				if (bug.isSame(anotherBug)) {
					newBug = false;
					break;
				}
			}
			if(newBug)
				result.add(bug);
		}
		return result;
	}
}
