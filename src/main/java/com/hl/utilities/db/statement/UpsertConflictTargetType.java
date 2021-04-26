package com.hl.utilities.db.statement;

@Deprecated
public enum UpsertConflictTargetType {
	CONSTRAINT("CONSTRAINT"),
	COLUMNS("COLUMNS"),
	WHERE_PREDICATE("WHERE_PREDICATE");
	
	private String targetType;
	UpsertConflictTargetType(String targetType){
		this.targetType = targetType;
	}
	public String getTargetType() {
		return this.targetType;
	}
}
