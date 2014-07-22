package me.littlepanda.hadoop-mongo;

import java.io.FilterInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import com.mongodb.gridfs.GridFSDBFile;

/**
 * @author 张静波 myplaylife@icloud.com
 * 实现了Seekable和Positionable接口，能够查找和定位流
 */
public class GridFSDBFileInputStream extends FilterInputStream implements Seekable, PositionedReadable {

	GridFSDBFile.MyInputStream gridFSInputStream;
	GridFSDBFile gridFile;

	public GridFSDBFileInputStream(GridFSDBFile gridFile) {
		super(gridFile.getInputStream());
		this.gridFile = gridFile;
		this.gridFSInputStream = (GridFSDBFile.MyInputStream) in;
	}

	/**
	 * @return current positon
	 * @throws IOException
	 */
	public long getPos() throws IOException {
		return gridFSInputStream.getPos();
	}
	
	/**
	 * @param pos 要去的位置
	 * @throws IOException
	 */
	public void seek(long pos) throws IOException {
		isValidPos(pos);
		int index = (int) (pos / gridFile.getChunkSize());
		gridFSInputStream.setCurrentChunkIdx(index);
		gridFSInputStream.setOffset((int) (pos % gridFile.getChunkSize()));
		if(index != gridFSInputStream.getCurrentChunkIdx()){
			gridFSInputStream.setData(gridFile.getChunk(index));
		}
	}
	
	/**
	 * not support other source
	 */
	public boolean seekToNewSource(long targetPos) throws IOException {
		return false;
	}
	
	@Override
	public int read() throws IOException {
		return in.read();
	}
	
	/**
	 * Read upto the specified number of bytes, from a given
	 * position within a file, and return the number of bytes read. This does not
	 * change the current offset of a file, and is thread-safe.
	 */
	public int read(long position, byte[] buffer, int offset, int length)
		throws IOException {
		isValidPos(position);
		in.mark(buffer.length - offset);
		this.seek(position);
		int r = in.read(buffer, offset, length);
		in.reset();
		return r;
	}
	
	/**
	 * Read the specified number of bytes, from a given
	 * position within a file. This does not
	 * change the current offset of a file, and is thread-safe.
	 */
	public void readFully(long position, byte[] buffer) throws IOException {
		this.read(position, buffer, 0, buffer.length);
	}
	
	/**
	 * Read number of bytes equalt to the length of the buffer, from a given
	 * position within a file. This does not
	 * change the current offset of a file, and is thread-safe.
	 */
	public void readFully(long position, byte[] buffer, int offset,
		int length) throws IOException {
		this.read(position, buffer, offset, length);
	}
	
	/**
	 * check the position value
	 */
	private void isValidPos(long pos){
		if (pos > gridFile.getLength()) {
			throw new RuntimeException(
					"seek pos is longer than data length!");
		}
		if (pos <= 0) {
			throw new RuntimeException("seek pos can't be or less than zero!");
		}
	}
}
