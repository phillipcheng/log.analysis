package etl.engine;

//used when the dataType has path, describe the content within the file, which inputFormat
public enum InputFormatType {
	Line,
	Text,
	SequenceFile,
	XML,
	CombineXML,
	FileName,//used for input is a directory
	Binary,//
	Section,//for kcv
	Mixed,//join will follow
}
