package etl.engine.types;

public enum AggrOperator {
	
	count,
	sum,
	avg,
	min,
	max,
	group,
	keep //do nothing, used to output joined table

}
