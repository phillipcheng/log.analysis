package etl.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;

public class XmlInputStream extends InputStream implements Seekable {
	
	private InputStream in;
	private long start;
	private long end;
	private long pos;
	private boolean withinBlock;
	private boolean matching;
	
	public XmlInputStream(InputStream in, long start, long end) {
		this.in = in;
		this.start = start;
		this.end = end;
		this.pos = 0;
	}
	
	public synchronized void reset() throws IOException {
		if (in instanceof FSDataInputStream)
			((FSDataInputStream) in).seek(start);
		else {
			pos = 0;
			in.reset();
		}
	}

	public int read() throws IOException {
		if (in instanceof FSDataInputStream) {
			if (!withinBlock && !matching && ((FSDataInputStream) in).getPos() >= end)
				/* see if we've passed the stop point: */
				return -1;
			else
				return in.read();
		} else {
			pos = pos + 1;
			return in.read();
		}
	}

	public boolean isWithinBlock() {
		return withinBlock;
	}

	public void setWithinBlock(boolean withinBlock) {
		this.withinBlock = withinBlock;
	}

	public boolean isMatching() {
		return matching;
	}

	public void setMatching(boolean matching) {
		this.matching = matching;
	}

	public void seek(long pos) throws IOException {
		if (in instanceof FSDataInputStream)
			((FSDataInputStream) in).seek(pos);
	}

	public long getPos() throws IOException {
		if (in instanceof FSDataInputStream)
			return ((FSDataInputStream) in).getPos();
		else
			return start + pos;
	}

	public boolean seekToNewSource(long targetPos) throws IOException {
		if (in instanceof FSDataInputStream)
			return ((FSDataInputStream) in).seekToNewSource(targetPos);
		else
			return false;
	}

	public void close() throws IOException {
		pos = 0;
		in.close();
	}

	public long getEnd() {
		return end;
	}
}
