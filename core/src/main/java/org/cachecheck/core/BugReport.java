package org.cachecheck.core;

public class BugReport {
	public String rdd; // Including the rdd name, transform operator, and the generation code position
	public Pattern pattern; 
	public String[] p_upinformation = {"none", "none"}; // the first is the position of persist, the second is the position of unpersist
	public String firstUseAction = "";
	public String lastUseAction = "";
	
	public boolean isSame(BugReport bugReport) {
		if(this.pattern != bugReport.pattern || !this.rdd.equals(bugReport.rdd))
			return false;
		switch (this.pattern) {
		case MissingPersist:
			return true;
		case LaggingPersist:
		case UnnecessaryPersist:
		case MissingUnpersist:
			return this.p_upinformation[0].equals(bugReport.p_upinformation[0]);
		case LaggingUnpersist:
		case PrematureUnpersist:
			return this.p_upinformation[1].equals(bugReport.p_upinformation[1]);
		default:
			return false;
		}
	}
	
	public String toString() {
		return rdd + "\t " + pattern + "\t " + p_upinformation[0] + "\t " 
				+ p_upinformation[1] + "\t " + firstUseAction + "\t " + lastUseAction;
	}
}
