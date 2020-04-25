package org.cachecheck.core;

public enum Pattern {
	MissingPersist("Missing persist"), 
	LaggingPersist("Lagging persist"), 
	UnnecessaryPersist("Unnecessary persist"), 
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
	
	public static Pattern getPattern(String value) {
		for(Pattern pattern : Pattern.values()) {
			if(value.equals(pattern.patternType))
				return pattern;
		}
		return null;
	}
}
