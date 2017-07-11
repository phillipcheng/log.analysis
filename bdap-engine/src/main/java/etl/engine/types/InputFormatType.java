package etl.engine.types;

//used when the dataType has path, describe the content within the file, which inputFormat
public enum InputFormatType {
	Line,
	Text,
	SequenceFile,
	ParquetFile,
	XML,
	CombineXML,
	FileName,//used for input is a directory
	CombineFileName,
	CombineWithFileNameText,
	Binary,//
	Section,//for kcv
	Mixed,//join will follow
	StaXML,
	
}
