package etl.engine;

public enum AggrOperator {
	
	count,
	sum,
	avg,
	min,
	max,
	group,
	keep //do nothing, used to output joined table

}
