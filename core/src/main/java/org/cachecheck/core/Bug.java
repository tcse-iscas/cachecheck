package org.cachecheck.core;

public class Bug {
	public int rddID;
	public Pattern pattern;
	public String location;
	
	public Bug(int rddID, Pattern bugPattern, String bugLocation) {
		this.rddID = rddID;
		this.pattern = bugPattern;
		this.location = bugLocation;
	}
	
	public boolean isSame(Bug anotherBug) {
		return (this.pattern.equals(anotherBug.pattern)) 
				&& (this.location.equals(anotherBug.location));
	}
	
	public Bug setLocation(String bugLocation) {
		if(location.equals(""))
			location = bugLocation;
		return this;
	}
}
