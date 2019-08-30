package cachecheck;

public enum Pattern {
	MissingPerssit("Missing persist"), 
	LaggingPersist("Lagging persist"), 
	UnnecessaryPersist("Lagging persist"), 
	MissingUnpersist("Missing unpersist"), 
	PrematureUnpersist("Premature unpersist"), 
	LaggingUnpersist("Lagging unpersist");
	private String patternType;
	private Pattern(String pattern) {
		this.patternType = pattern;
	}
	
	@Override
	public String toString() {
		return this.patternType;
	}
	
	public boolean equals(Pattern pattern) {
		return patternType.equals(pattern.patternType);
	}
}
