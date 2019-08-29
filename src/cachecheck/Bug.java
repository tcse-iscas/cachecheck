package cachecheck;

public class Bug {
	public int rddID;
	public Pattern pattern;
	public String location;
	
	public Bug(int rddID, Pattern bugPattern, String bugLocation) {
		this.rddID = rddID;
		this.pattern = bugPattern;
		this.location = bugLocation;
	}
}
