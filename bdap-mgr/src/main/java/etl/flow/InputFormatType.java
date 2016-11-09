package etl.flow;

//describe the content of the data and how it want to be processed
public enum InputFormatType {
	FileName, //input is a folder, recursively get all the file names, processed one file per map
	File, //input are contents of the file, processed line by line, map number controlled by file size
	Line, //input are contents of the file, processed one line per map, spark:the number of partition equals to the number of line
}
