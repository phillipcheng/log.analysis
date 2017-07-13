package etl.cmd;

import java.io.BufferedInputStream;
import java.io.FilenameFilter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.ETLCmd;
import etl.engine.types.OutputFormat;
import etl.engine.types.ProcessMode;
import etl.util.ConfigKey;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;
import scala.Tuple2;

public class UnpackCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;
	private static final String SEQ_FILENAME_EXT = ".seq";
	public static final Logger logger = LogManager.getLogger(UnpackCmd.class);
	public static final @ConfigKey String cfgkey_input_file_filter="input.file.filter";
	public static final @ConfigKey String cfgkey_output_file_filter="output.file.filter";
	public static final @ConfigKey(type=Boolean.class) String cfgkey_file_append="filename.append";
	private static final byte[] NEW_LINE = "\n".getBytes(StandardCharsets.UTF_8);
	
	private FilenameFilter inputFileFilter;
	private FilenameFilter outputFileFilter;
	private boolean sequenceFile;
	private boolean filenameAppend;
	
	public UnpackCmd(){
		super();
	}
	
	public UnpackCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs) {
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public UnpackCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public UnpackCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs) {
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}

	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);

		String fileFilterExp;
		String fileFilter;
		
		OutputFormat unpackFileFormat = OutputFormat.valueOf(super.getCfgString(cfgkey_output_file_format, OutputFormat.text.toString()));
		if (unpackFileFormat == OutputFormat.sequence)
			sequenceFile = true;
		else
			sequenceFile = false;
		
		fileFilterExp = super.getCfgString(cfgkey_input_file_filter, null);
		if (fileFilterExp != null) {
			fileFilter = (String) ScriptEngineUtil.eval(fileFilterExp, VarType.STRING, super.getSystemVariables());
			if (fileFilter != null && fileFilter.length() > 0)
				this.inputFileFilter = new WildcardFileFilter(fileFilter.split(" "));
			else
				this.inputFileFilter = TrueFileFilter.TRUE;
		} else {
			this.inputFileFilter = TrueFileFilter.TRUE;
		}
		
		fileFilterExp = super.getCfgString(cfgkey_output_file_filter, null);
		if (fileFilterExp != null) {
			fileFilter = (String) ScriptEngineUtil.eval(fileFilterExp, VarType.STRING, super.getSystemVariables());
			if (fileFilter != null && fileFilter.length() > 0)
				this.outputFileFilter = new WildcardFileFilter(fileFilter.split(" "));
			else
				this.inputFileFilter = TrueFileFilter.TRUE;
		} else {
			this.outputFileFilter = TrueFileFilter.TRUE;
		}
		filenameAppend=getCfgBoolean(cfgkey_file_append, false);
	}

	public boolean hasReduce() {
		return false;
	}

	//tfName not used
	public List<Tuple2<String, String>> flatMapToPair(String tfName, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		super.init();
		//value is the src zip file path
		List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
		SequenceFile.Writer writer = null;
		FSDataInputStream fsIn = null;
		BufferedInputStream bufIn = null;
		ArchiveInputStream in = null;
		BufferedInputStream bufCompIn = null;
		CompressorInputStream compIn = null;
		FSDataOutputStream fsOut;
		ArchiveEntry entry;
		String destFile;
		String fileName;
		int i;
		long longKey;
		LongWritable key;
		Text value;
        byte[] buffer;
		
		String outputFolder = "";
		if (context!=null){
			outputFolder = context.getConfiguration().get(FileOutputFormat.OUTDIR);
			if (!outputFolder.endsWith(Path.SEPARATOR))
				outputFolder = outputFolder + Path.SEPARATOR;
		}
		Path path = new Path(row);
		if (inputFileFilter.accept(null, path.getName())) {
			try {
				fsIn = fs.open(path);
				bufIn = new BufferedInputStream(fsIn);
				try {
					compIn = new CompressorStreamFactory().createCompressorInputStream(bufIn);
					bufCompIn = new BufferedInputStream(compIn);
					in = new ArchiveStreamFactory().createArchiveInputStream(bufCompIn);
				} catch (Exception e) {
					logger.debug(e.getMessage() + " - " + path);
				}
				
				if (in == null)
					in = new ArchiveStreamFactory().createArchiveInputStream(bufIn);
				
				if (sequenceFile) {
					int bufferSizeValue = fs.getConf().getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
							CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
					i = path.getName().indexOf('.');
					if (i > 0)
						fileName = path.getName().substring(0, i);
					else
						fileName = path.getName();
					destFile = outputFolder + fileName + SEQ_FILENAME_EXT;
					if (context!=null){
						writer = SequenceFile.createWriter(getHadoopConf(), SequenceFile.Writer.file(new Path(destFile)),
							SequenceFile.Writer.keyClass(LongWritable.class),
							SequenceFile.Writer.valueClass(Text.class),
							SequenceFile.Writer.bufferSize(bufferSizeValue));
					}
					buffer = new byte[bufferSizeValue]; /* Read buffer size set to the same as hdfs io file buffer size */
					
					longKey = 0;
					while ((entry = in.getNextEntry()) != null) {
						if (!entry.isDirectory() && outputFileFilter.accept(null, entry.getName())) {
							if(filenameAppend){
								destFile = outputFolder + new StringBuffer(entry.getName())
										.insert(entry.getName().lastIndexOf("/")+1, fileName+".");
							}else{
								destFile = outputFolder + entry.getName();
							}	
				            key = new LongWritable(longKey);
				            value = new Text();
				            value.set(destFile);
				            value.append(NEW_LINE, 0, NEW_LINE.length);
							while ((i = IOUtils.read(in, buffer)) > 0) {
								value.append(buffer, 0, i);
								if (i < bufferSizeValue) /* EOF reached */
									break;
							}
							if (context!=null){
								writer.append(key, value);
							}else{
								ret.add(new Tuple2<String, String>(destFile, value.toString()));
							}
						}
					}
				} else {
					while ((entry = in.getNextEntry()) != null) {
						if (!entry.isDirectory() && outputFileFilter.accept(null, entry.getName())) {
							if(filenameAppend){
								i = path.getName().indexOf('.');
								if (i > 0)
									fileName = path.getName().substring(0, i);
								else
									fileName = path.getName();
								destFile = outputFolder + fileName + "." + entry.getName();
							}else{
								destFile = outputFolder + entry.getName();
							}
							fsOut = null;
							try {
								fsOut = fs.create(new Path(destFile));
								IOUtils.copy(in, fsOut);
							} finally {
								if (fsOut != null)
									fsOut.close();
							}
						}
					}
				}
			} finally {
				if (writer != null)
			         org.apache.hadoop.io.IOUtils.closeStream(writer);
				if (in != null)
					in.close();
				if (bufCompIn != null)
					bufCompIn.close();
				if (compIn != null)
					compIn.close();
				if (bufIn != null)
					bufIn.close();
				if (fsIn != null)
					fsIn.close();
			}
		}
		return ret;
	}
	
	public Map<String, Object> mapProcess(long offset, String row,
			Mapper<LongWritable, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception {
		flatMapToPair(null, row, context);
		return null;
	}
}
