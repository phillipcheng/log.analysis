package etl.flow.mgr;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.springframework.core.io.Resource;

import bdap.util.FileType;

public class LogResource implements Resource {
	private Resource wrappedObj;
	private String filePath;
	private FileType fileType;
	
	private static class LogInputStream extends InputStream {
		private InputStream wrappedIn;
		private int index;
		private int len;
		private boolean end;
		private byte[] buffer;
		private final static byte[] PRE_BYTES = "<pre>".getBytes(StandardCharsets.UTF_8);
		private final static byte[] PRE_CLOSE_BYTES = "</pre>".getBytes(StandardCharsets.UTF_8);
		public LogInputStream(InputStream in) throws IOException {
			wrappedIn = in;
			index = 0;
			len = 0;
			end = false;
			buffer = new byte[PRE_CLOSE_BYTES.length];
			reset();
		}
		public int read() throws IOException {
			if (end) {
				return -1;
				
			} else if (len > 0) {
				int v = buffer[index];
				index ++;
				if (index >= len) {
					len = 0;
					index = 0;
				}
				return v;
				
			} else {
				int v = wrappedIn.read();
				if (v != -1) {
					byte b = (byte) v;
					while (b == PRE_CLOSE_BYTES[index]) {
						index ++;
						if (index >= PRE_CLOSE_BYTES.length) {
							index = 0;
							end = true;
							return -1;
						}
						b = (byte) wrappedIn.read();
						buffer[index-1] = b;
					}
					len = index;
					index = 0;
				}
				return v;
			}
		}
		public long skip(long n) throws IOException {
			return wrappedIn.skip(n);
		}
		public int available() throws IOException {
			return wrappedIn.available();
		}
		public void close() throws IOException {
			wrappedIn.close();
		}
		public synchronized void mark(int readlimit) {
			wrappedIn.mark(readlimit);
		}
		public synchronized void reset() throws IOException {
			end = false;
			wrappedIn.reset();
			int v;
			byte b;
			int i = 0;
			while((v = wrappedIn.read()) != -1) {
				b = (byte) v;
				if (b == PRE_BYTES[i]) {
					i ++;
					if (i >= PRE_BYTES.length)
						break;
				} else {
					i = 0;
				}
			}
		}
		public boolean markSupported() {
			return wrappedIn.markSupported();
		}
	}
	
	public LogResource(Resource r, String filePath, FileType fileType) {
		wrappedObj = r;
		this.filePath = filePath;
		this.fileType = fileType;
	}
	
	public InputStream getInputStream() throws IOException {
		return new LogInputStream(wrappedObj.getInputStream());
	}

	public boolean exists() {
		return wrappedObj.exists();
	}

	public boolean isReadable() {
		return wrappedObj.isReadable();
	}

	public boolean isOpen() {
		return wrappedObj.isOpen();
	}

	public URL getURL() throws IOException {
		return wrappedObj.getURL();
	}

	public URI getURI() throws IOException {
		return wrappedObj.getURI();
	}

	public File getFile() throws IOException {
		return wrappedObj.getFile();
	}

	public long contentLength() throws IOException {
		return wrappedObj.contentLength();
	}

	public long lastModified() throws IOException {
		return wrappedObj.lastModified();
	}

	public Resource createRelative(String relativePath) throws IOException {
		return wrappedObj.createRelative(relativePath);
	}

	public String getFilename() {
		return filePath;
	}

	public String getDescription() {
		return wrappedObj.getDescription();
	}
	
	public FileType getFiletype() {
		return fileType;
	}

}
