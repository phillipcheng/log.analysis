package etl.cmd;

import java.io.BufferedInputStream;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class UnpackCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LogManager.getLogger(UnpackCmd.class);
	public static final String cfgkey_input_file_filter="input.file.filter";
	public static final String cfgkey_output_file_filter="output.file.filter";
	
	private FilenameFilter inputFileFilter;
	private FilenameFilter outputFileFilter;
	
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
	}

	public Map<String, Object> mapProcess(long offset, String row,
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		List<String> fileEntries = new ArrayList<String>();
		Path path = new Path(row);
		FSDataInputStream fsIn = null;
		BufferedInputStream bufIn = null;
		ArchiveInputStream in = null;
		FSDataOutputStream fsOut;
		ArchiveEntry entry;
		String destFile;
		
		String outputFolder = context.getConfiguration().get(FileOutputFormat.OUTDIR);
		if (!outputFolder.endsWith(Path.SEPARATOR))
			outputFolder = outputFolder + Path.SEPARATOR;
		
		if (inputFileFilter.accept(null, path.getName())) {
			try {
				fsIn = fs.open(path);
				bufIn = new BufferedInputStream(fsIn);
				in = new ArchiveStreamFactory().createArchiveInputStream(bufIn);
				
				while ((entry = in.getNextEntry()) != null) {
					if (!entry.isDirectory() && outputFileFilter.accept(null, entry.getName())) {
						destFile = outputFolder + entry.getName();
						fsOut = null;
						try {
							fsOut = fs.create(new Path(destFile));
							IOUtils.copy(in, fsOut);
							fileEntries.add(destFile);
						} finally {
							if (fsOut != null)
								fsOut.close();
						}
					}
				}
				
			} finally {
				if (in != null)
					in.close();
				if (bufIn != null)
					bufIn.close();
				if (fsIn != null)
					fsIn.close();
			}
		}

		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put(ETLCmd.RESULT_KEY_LOG, fileEntries);
		return retMap;
	}

}
