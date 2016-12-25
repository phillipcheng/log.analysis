package etl.engine;

//used when the dataType has path, describe the content within the file, which inputFormat
public enum InputFormatType {
	Line,
	XML,
	Text,
	Binary,//
	Section,//for kcv
	Mixed,//join will follow
	FileName,//used for input is a directory
}
