// GridFSDBFile.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb.gridfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.Util;

/**
 * This class enables to retrieve a GridFS file metadata and content.
 * Operations include:
 * - writing data to a file on disk or an OutputStream
 * - getting each chunk as a byte array
 * - getting an InputStream to stream the data into
 * @author antoine
 * 
 * update by 张静波 zhang.jb@neusoft.com
 * 将MyInputStream暴露给出来
 * 实现了mark()和reset()方法
 * 增加了输出流，用来在文件中追加内容
 * 	获取最后一个块
 * 	替换最后一个块
 * 	剩余内容继续添加块
 * 
 * 此类原型来自mongo-java-drvierv2.11.2，如果升级驱动版本，请连带升级此类
 */
public class GridFSDBFile extends GridFSFile {
	
	/*
	 * outputstream write buffer
	 */
	private byte[] _buffer = null;
	/*
	 * 当前chunk索引号
	 */
	private int _currentChunkIdx = 0;
	/*
	 * 写缓冲区当前位置
	 */
	private int _currentBufferPosition = 0;
	
	private MessageDigest _messageDigester = null;
	
    /**
     * Returns an InputStream from which data can be read
     * @return
     */
    public InputStream getInputStream(){
        return new MyInputStream();
    }
    
    /**
     * add by 张静波 zhang.jb@neusoft.com
     * Returns an OutputStream from which data can be append
     * @return
     */
    public OutputStream getOutputStream(){
    	_buffer = new byte[(int) _chunkSize];
    	if(numChunks() > 0){
    		_currentChunkIdx = numChunks() - 1;
    	}
    	byte[] temp = getChunk(_currentChunkIdx);
    	System.arraycopy(temp, 0, _buffer, 0, temp.length);
    	_currentBufferPosition = temp.length;
    	try {
            _messageDigester = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("No MD5!");
        }
        _messageDigester.reset();
        //删除最后一个块
        _fs._chunkCollection.remove(BasicDBObjectBuilder.start( "files_id" , _id )
                .add( "n" , _currentChunkIdx ).get());
        //因为删除了最后一个块，所以文件的长度也相应的减少了
        _length -= temp.length;
    	return new MyOutputStream();
    }
    /**
     * Writes the file's data to a file on disk
     * @param filename the file name on disk
     * @return
     * @throws IOException
     * @throws MongoException 
     */
    public long writeTo( String filename ) throws IOException {
        return writeTo( new File( filename ) );
    }
    /**
     * Writes the file's data to a file on disk
     * @param f the File object
     * @return
     * @throws IOException
     * @throws MongoException 
     */
    public long writeTo( File f ) throws IOException {
        
    	FileOutputStream out = null;
    	try{
    		out = new FileOutputStream( f );
    		return writeTo( out);
    	}finally{
    	    if(out != null)
    	        out.close();
    	}
    }

    /**
     * Writes the file's data to an OutputStream
     * @param out the OutputStream
     * @return
     * @throws IOException
     * @throws MongoException 
     */
    public long writeTo( OutputStream out )
    		throws IOException {
    	final int nc = numChunks();
    	for ( int i=0; i<nc; i++ ){
    	    out.write( getChunk( i ) );
    	}
    	return _length;
    }
    
    /**
     * update by 张静波 zhang.jb@neusoft.com
     * add public at front
     * @param i
     * @return
     */
    public byte[] getChunk( int i ){
        if ( _fs == null )
            throw new RuntimeException( "no gridfs!" );
        
        DBObject chunk = _fs._chunkCollection.findOne( BasicDBObjectBuilder.start( "files_id" , _id )
                                                       .add( "n" , i ).get() );
        if(getLength() == 0){
        	return new byte[0];
        }
        if ( chunk == null )
            throw new MongoException( "can't find a chunk!  file id: " + _id + " chunk: " + i );

        return (byte[])chunk.get( "data" );
    }
    
    void remove(){
        _fs._filesCollection.remove( new BasicDBObject( "_id" , _id ) );
        _fs._chunkCollection.remove( new BasicDBObject( "files_id" , _id ) );
    }

    public class MyInputStream extends InputStream {

        MyInputStream(){
            _numChunks = numChunks();
        }
        /**
         * update by 张静波 zhang.jb@neusoft.com
         * now can read stream like a whole stream
         * 如果剩余字节数大于2G，返回-1；否则返回正确的字节数。
         */
        public int available(){
        	long available = getLength() - getPos();
        	return available > Integer.MAX_VALUE ? -1 : (int)available;
        	/*
        	 * 下面是原实现
        	 */
//            if ( _data == null )
//                return 0;
//            return _data.length - _offset;
        }
        
        public void close(){
        }
        
        /**
         * update by 张静波 zhang.jb@neusoft.com
         * support mark&reset now
         */
        public void mark(int readlimit){
            _mark_currentChunkIdx = _currentChunkIdx;
            _mark_data = _data;
            _mark_offset = _offset;
            _mark_readlimit = readlimit;
            _isMarkNow = true;
        }
        public void reset(){
        	_currentChunkIdx = _mark_currentChunkIdx;
            _offset = _mark_offset;
            _data = _mark_data;
            clearMark();
        }
        public boolean markSupported(){
            return true;
        }

        public int read(){
        	//add by 张静波 zhang.jb@neusoft.com
        	doMark(1);
            byte b[] = new byte[1];
            int res = read( b );
            if ( res < 0 )
                return -1;
            return b[0] & 0xFF;
        }
        
        public int read(byte[] b){
            return read( b , 0 , b.length );
        }
        /**
         * update by 张静波 zhang.jb@neusoft.com
         * now can read stream like a whole stream
         */
        public int read(byte[] b, int off, int len){
        	long chunkSize = getChunkSize();
        	if(_currentChunkIdx + 1 == numChunks() && _offset == getLength() % chunkSize){
        		return -1;
        	}
        	if(getPos() + len > getLength()){
        		len = (int) (getLength() - getPos());
        	}
        	if((chunkSize - _offset) >= len){ //当不跨chunk时
        		_data = getChunk(_currentChunkIdx);
        		System.arraycopy( _data , _offset , b , off , len );
        		_offset += len;
        	} else { //跨chunk时
        		int endIdx = _currentChunkIdx + (int) ((len - chunkSize + _offset) / chunkSize) + 1;
        		int endOffset = (int) ((len - chunkSize + _offset) % chunkSize);
        		int readSize = (int)(chunkSize - _offset);
        		//首先把第一个块的内容读进来
        		System.arraycopy( _data , _offset , b , off , readSize );
        		off += readSize;
        		//然后读后续完整个块
        		while(++_currentChunkIdx < endIdx){
        			_data = getChunk(_currentChunkIdx);
        			System.arraycopy( _data , 0 , b , off , (int)chunkSize );
        			off += chunkSize;
        		}
        		//最后把最后一个块的内容读进来
        		_data = getChunk(_currentChunkIdx);
        		System.arraycopy( _data , 0 , b , off , endOffset );
        		_offset = endOffset;
        	}
        	return len;
        	
        	/*
        	 * 下面是原实现 
        	 */
//            if ( _data == null || _offset >= _data.length ){
//                if ( _currentChunkIdx + 1 >= _numChunks )
//                    return -1;
//                
//                _data = getChunk( ++_currentChunkIdx );
//                _offset = 0;
//            }
//
//            int r = Math.min( len , _data.length - _offset );
//            //add by 张静波 zhang.jb@neusoft.com
//            doMark(r);
//            
//            System.arraycopy( _data , _offset , b , off , r );
//            _offset += r;
//            return r;
        }
        
        /**
         * add by 张静波 zhang.jb@neusoft.com
         * @return 当前流所处位置
         */
        public long getPos(){
        	return _chunkSize * _currentChunkIdx + _offset;
        }

        /**
         * Will smartly skips over chunks without fetching them if possible.
         */
        public long skip(long numBytesToSkip) throws IOException {
            if (numBytesToSkip <= 0)
                return 0;

            if (_currentChunkIdx == _numChunks)
                //We're actually skipping over the back end of the file, short-circuit here
                //Don't count those extra bytes to skip in with the return value
                return 0;

            // offset in the whole file
            long offsetInFile = 0;
            if (_currentChunkIdx >= 0)
                offsetInFile = _currentChunkIdx * _chunkSize + _offset;
            if (numBytesToSkip + offsetInFile >= _length) {
                _currentChunkIdx = _numChunks;
                _data = null;
                return _length - offsetInFile;
            }

            int temp = _currentChunkIdx;
            _currentChunkIdx = (int)((numBytesToSkip + offsetInFile) / _chunkSize);
            if (temp != _currentChunkIdx)
                _data = getChunk(_currentChunkIdx);
            _offset = (int)((numBytesToSkip + offsetInFile) % _chunkSize);

            return numBytesToSkip;
        }

        final int _numChunks;

        int _currentChunkIdx = 0;
        int _offset = 0;
        byte[] _data = null;
        
        /*
         * add by 张静波 zhang.jb@neusoft.com
         * save the mark position
         */
        boolean _isMarkNow = false;
        int _mark_currentChunkIdx = -1;
        int _mark_offset = 0;
        byte[] _mark_data = null;
        int _mark_readlimit = -1;
        
        private void doMark(int limit){
        	if(_isMarkNow){
        		_mark_readlimit -= limit;
        		if(_mark_readlimit < 0){
        			throw new RuntimeException("mark read beyond limit");
        		}
        	}
        }
        
        private void clearMark(){
        	_mark_currentChunkIdx = -1;
            _mark_offset = 0;
            _mark_data = null;
            _mark_readlimit = -1;
            _isMarkNow = false;
        }
        
        
        public int getCurrentChunkIdx(){
        	return _currentChunkIdx;
        }
        
        public int getOffset(){
        	return _offset;
        }
        
        public byte[] getData(){
        	return _data;
        }
        
        public void setCurrentChunkIdx(int index){
        	_currentChunkIdx = index;
        }
        
        public void setOffset(int offset){
        	_offset = offset;
        }
        
        public void setData(byte[] data){
        	_data = data;
        }
    }
    
    /**
     * @author 张静波 zhang.jb@neusoft.com
     * 一个能够追加数据的流
     */
    class MyOutputStream extends OutputStream {

		@Override
		public void write(int b) throws IOException {
			byte[] byteArray = new byte[1];
            byteArray[0] = (byte) (b & 0xff);
            write( byteArray, 0, 1 );
		}

        /**
         * {@inheritDoc}
         *
         * @see java.io.OutputStream#write(byte[], int, int)
         */
        @Override
        public void write( byte[] b , int off , int len ) throws IOException {
            int offset = off;
            int length = len;
            int toCopy = 0;
            while ( length > 0 ) {
                toCopy = length;
                if ( toCopy > _chunkSize - _currentBufferPosition ) {
                    toCopy = (int) _chunkSize - _currentBufferPosition;
                }
                System.arraycopy( b, offset, _buffer, _currentBufferPosition, toCopy );
                _currentBufferPosition += toCopy;
                offset += toCopy;
                length -= toCopy;
                if ( _currentBufferPosition == _chunkSize ) {
                    _dumpBuffer( false );
                }
            }
        }
        
        /**
         * Dumps a new chunk into the chunks collection. Depending on the flag, also
         * partial buffers (at the end) are going to be written immediately.
         *
         * @param writePartial
         *            Write also partial buffers full.
         * @throws MongoException 
         */
        private void _dumpBuffer( boolean writePartial ) {
            if ( ( _currentBufferPosition < _chunkSize ) && !writePartial ) {
                // Bail out, chunk not complete yet
                return;
            }
            if (_currentBufferPosition == 0) {
                // chunk is empty, may be last chunk
                return;
            }

            byte[] writeBuffer = _buffer;
            if ( _currentBufferPosition != _chunkSize ) {
                writeBuffer = new byte[_currentBufferPosition];
                System.arraycopy( _buffer, 0, writeBuffer, 0, _currentBufferPosition );
            }

            DBObject chunk = createChunk(_id, _currentChunkIdx, writeBuffer);

            _fs._chunkCollection.save( chunk );

            _currentChunkIdx++;
            _length += writeBuffer.length;
            _messageDigester.update( writeBuffer );
            _currentBufferPosition = 0;
        }
        
        private DBObject createChunk(Object id, int currentChunkNumber, byte[] writeBuffer) {
             return BasicDBObjectBuilder.start()
             .add("files_id", id)
             .add("n", currentChunkNumber)
             .add("data", writeBuffer).get();
        }

        /**
         * Processes/saves all data from {@link java.io.InputStream} and closes
         * the potentially present {@link java.io.OutputStream}. The GridFS file
         * will be persisted afterwards.
         */
        @Override
        public void close() { 
            // write last buffer if needed
            _dumpBuffer( true );
            // finish stream
            _finishData();
            // save file obj
            GridFSDBFile.super.save();
        }
        
        private void _finishData() {
        	_md5 = Util.toHex( _messageDigester.digest() );
        	_messageDigester = null;
        }
    }
}
