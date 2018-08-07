/*
* Copyright © 2017 - 2018 Software AG, Darmstadt, Germany and/or its licensors
*
* SPDX-License-Identifier: Apache-2.0
*
*   Licensed under the Apache License, Version 2.0 (the "License");
*   you may not use this file except in compliance with the License.
*   You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.                                                            
*
*/

package tom;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Observable;
import java.util.Set;
import java.util.TreeMap;


//import net.sf.ehcache.Cache;
//import net.sf.ehcache.CacheManager;
//import net.sf.ehcache.Element;

import com.eaio.stringsearch.BoyerMooreSunday;

public class Nio extends Observable {
	
	public static FileSystem FS = FileSystems.getDefault();
	public static Path SRC_PATH;
	public static String SRC_FILENAME;
	public static Path DEST_ROOT_PATH;
//	public static String CACHE_NAME="NioCache";
//	public static String CACHE_MGR_CONFIG="./ehcache.xml";
	public static long MAX_FILES_PER_DIR = 2;
	public static String CHUNK_FILE_PREFIX="chunk";
	public static String BYTE_BUFFER_HANDLER_CLASSNAME = "BufferHandlerSampleImpl";	// testing purpose only!!!
//	final CacheManager cacheManager = CacheManager.newInstance(CACHE_MGR_CONFIG);
//	final Cache cache = cacheManager.getCache(CACHE_NAME);
	
	private class NioPattern {
		public byte[] p;
		public byte[] rc;
		
		public NioPattern(byte[] p, byte[] rc) {
			this.p = p;
			this.rc = rc;
		}
	}
	
	private LinkedHashMap<byte[],byte[]> patternList;
	private int patternIndx = 0;
	private Set<byte[]> allKeys = null;
	private int maxPatternLen = -1;
	private int maxRcLen = -1;
	private TreeMap<Long,ArrayList<Object>> indexList = null;
	
	public void setPatternList (LinkedHashMap<byte[],byte[]> pl) {
		patternList = pl;
		patternIndx = 0;
		allKeys = pl.keySet();
		allKeys.forEach( (k) -> 
		{ 
			if (k.length > maxPatternLen) maxPatternLen = k.length;
			byte[] val = pl.get(k);
			if (val != null && val.length > maxRcLen) maxRcLen = val.length;
		});
		//System.out.println("max pattern len = "+maxPatternLen+", max rc len = "+maxRcLen);
	}
	
	public void addPattern(byte[] p, byte[] rc) {
		if (patternList == null) {
			LinkedHashMap<byte[],byte[]> lhm = new LinkedHashMap<byte[],byte[]>();
			lhm.put(p, rc);
			setPatternList(lhm);
		} else {
			patternList.put(p,rc);
		}
		if (p.length > maxPatternLen) {
			maxPatternLen = p.length;
		}
		if (rc != null && rc.length > maxRcLen) {
			maxRcLen = rc.length;
		}
		//System.out.println("max pattern len = "+maxPatternLen);
		//System.out.println("max rc len = "+maxRcLen);
	}
	
	public NioPattern nextPattern() {
		byte[] key = null;
		try { key = (byte[]) (allKeys.toArray())[patternIndx]; } catch (Exception e){};
		if (key == null) {
			return null;
		}
		patternIndx++;
		//System.err.println("search for pattern: "+new String(key));
		return new NioPattern(key,patternList.get(key));
	}
	
	public long getChunkPos(int indx) {
		return (long) this.indexList.keySet().toArray()[indx];
	}
	
	public long getChunkLen(Long key) {
		ArrayList<Object> al = (ArrayList<Object>) this.indexList.get(key);
		if (al == null || al.isEmpty()) {
			return -2;
		}
		return (long) al.get(0);
	}
	
	public long getChunkLen(int indx) {
		ArrayList<Object> al = (ArrayList<Object>) this.indexList.get(this.indexList.keySet().toArray()[indx]);
		if (al == null || al.isEmpty()) {
			return -2;
		}
		return (long) al.get(0);
	}
	
	public void setChunkLen(Long key, Long val) {
		ArrayList<Object> al = (ArrayList<Object>) this.indexList.get( key );
		al.remove(0);
		al.add(0, val);
	}
	
	public byte[] getChunkPattern(Long key) {
		ArrayList<Object> al = (ArrayList<Object>) this.indexList.get( key );
		return (byte[]) al.get(1);
	}
	
	public byte[] getChunkPattern(int indx) {
		ArrayList<Object> al = (ArrayList<Object>) this.indexList.get(this.indexList.keySet().toArray()[indx]);
		return (byte[]) al.get(1);
	}
	
	public WritableByteChannel checkAndCreate(Path path, int dir, String filename) {
		Path retPath = dir != -1 ? Paths.get(path.toString(), ""+dir):path;
		WritableByteChannel wbc = null;
		if (Files.notExists(retPath, LinkOption.NOFOLLOW_LINKS) && dir != -1) {
			System.err.println(path.toAbsolutePath()+" doesn't exists.");
			try {
				Files.createDirectory(retPath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				return null;
			}
		}
		retPath = Paths.get(retPath.toString(),filename);
		if (Files.notExists(retPath, LinkOption.NOFOLLOW_LINKS)) {
			System.err.println(path.toAbsolutePath()+" doesn't exists.");
			try {
				wbc = Files.newByteChannel(retPath, EnumSet.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				return null;
			}
		} else {
			try {
				wbc = Files.newByteChannel(retPath, EnumSet.of(StandardOpenOption.WRITE));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				return null;
			}
		}
		return wbc;
	}
	
	public void bufferToFile(ByteBuffer buffer, WritableByteChannel wbc) throws IOException {
		//System.out.println("cap = "+buffer.capacity());
		if (buffer.capacity() == 0) {
			return;
		}
		buffer.flip();
		wbc.write(buffer);
		wbc.close();
	}
	
	public void printBuffer(ByteBuffer buf) {
		buf.flip();
		int i = 0;
		while(buf.hasRemaining()) {
			System.out.print(i+": "+(char)buf.get()+", ");
			i++;
		}
		System.out.println();
	}
	
	public int search (byte[] text, int start, int end, byte[] pattern) {
		BoyerMooreSunday bms = new BoyerMooreSunday();
		int pos = bms.searchBytes(text, start, end, pattern);
		return pos;
	}
	
	public void storeMetaInfo(String key, ByteBuffer bb, int offset, int len) {
		byte[] dst = new byte[len];
		bb.position(offset);
		bb.get(dst, 0, len);
		//System.out.println("-->"+new String(dst));
//		System.out.println("cache == "+cache);
//		cache.put(new Element(key,dst));
	}
	
	public byte[] getMetaInfo(String key) {
//		Element meta = cache.get(key);
//		return (byte[]) meta.getObjectValue();
		return null;
	}
	
	public boolean deleteMetaInfo (String key) {
//		return cache.remove(key);
		return false;
	}
	
	public void createDir(Path outPath, String dir) {
		Path tmpPath = Paths.get(outPath.toString(), dir);
		try {
			Files.createDirectory(tmpPath);
		} catch (IOException e) {
			System.err.println("ignoring exception "+e);
		}
	}
	
	private void printIndexList() {
		Set<Long> keySet = this.indexList.keySet();
		System.out.println("************ index list start *************************");
		keySet.forEach( (key) -> {
			ArrayList<?> al = (ArrayList<?>) this.indexList.get(key);
			System.out.println("key: "+key);
			al.forEach( (obj) -> {
				if (obj instanceof byte[]) System.out.println("\t pattern: "+new String((byte[])obj)); else System.out.println("\t len: "+obj) ;
			});
		});
		System.out.println("************ index list end *************************");
	}
	
	private void addIndexElement(Long currentKey, Long lastKey, Long len, byte[] pattern) {
		ArrayList<Object> al = new ArrayList<Object>();
		if (this.indexList == null) {
			this.indexList = new TreeMap<Long,ArrayList<Object>>();
		}
		al.add(new Long(-1));
		al.add(pattern);
		this.indexList.put(currentKey, al);
		//this.printIndexList();
	}
	
	private boolean hasReleaseChars(byte[] bb, long pos, byte[] rc) {
		boolean hasRc = true;
		if (rc == null || pos == -1) {
			return false;
		}
		for (int i = 0; i<rc.length && hasRc; i++){
			hasRc = (bb[(int)pos-i-1] == rc[rc.length-i-1]) ;
		};
		return hasRc;
	}
	
	private void updateList(long filesize) {
		Long prevKey = null;
		ArrayList<Object> al = null;
		if (this.indexList == null) {
			System.out.println("no pattern found -> nothing to do.");
			this.indexList = new TreeMap<Long,ArrayList<Object>>();
			return;
		}
		//System.out.println("first entry = "+this.indexList.firstEntry().getKey());
		if ( this.indexList.firstEntry().getKey() > 0 ) {	// return first chunk 
			al = new ArrayList<Object>();
			al.add(this.indexList.firstEntry().getKey()-1);
			al.add("_prefix_".getBytes());
			this.indexList.put(0L,al);
		}
		for (Long key : this.indexList.keySet()) {
			if (prevKey == null){
				prevKey = key;
				continue;
			}
			al = this.indexList.get(prevKey);
			al.remove(0);
			al.add(0,key-prevKey);
			prevKey=key;
		}
		Long lastKey = (Long) this.indexList.keySet().toArray()[this.indexList.keySet().size()-1];
		al = this.indexList.get(lastKey);
		al.remove(0);
		al.add(0,filesize-lastKey);
		//this.printIndexList();
	}
	
	public TreeMap<Long, ArrayList<Object>> determineChunks (FileChannel inChannel, long bufferSize) throws IOException {
		long currentPos = 0;
		Long lastKey = 0L;
		Long currentKey = 0L;
		Long len = -1L;
		ByteBuffer bb = ByteBuffer.allocate((int) bufferSize);
		int bytesRead = -1;
		NioPattern p = null;
		
		do {
			inChannel.position(currentPos);
			bytesRead = inChannel.read(bb);
			//System.out.println("bytes read = "+bytesRead);
			//System.out.println("buffer = "+new String(bb.array()));
			p = this.nextPattern();
			int end = bb.array().length;
			int start = 0;
			while (p != null) {
				int foundPos = this.search(bb.array(),start,end,p.p);
				if (this.hasReleaseChars(bb.array(), foundPos, p.rc)) {	// release char(s) found
					foundPos = -1;
				}
				if (foundPos != -1) {
					//System.out.println("found "+new String(p.p)+" at total pos "+(foundPos+currentPos));
					currentKey = new Long(foundPos+currentPos);
					len = new Long(currentKey.longValue()-lastKey.longValue());
					this.addIndexElement(currentKey, lastKey, len, p.p);
					lastKey = currentKey;
					start = (len.intValue() == 0) ? foundPos+p.p.length : foundPos+len.intValue();
					//System.out.println("new start: "+start);
					continue;
				}
				//System.out.println(indexList);
				p = this.nextPattern();
				bb.position(0);
				start = 0;
				currentKey = 0L;
				lastKey = 0L;
			}
			currentPos+=(bytesRead-(maxPatternLen+maxRcLen)+1);	// make sure that possible pattern is not split up into 2 buffers
			bb.clear();
			patternIndx=0;
		} while (inChannel.position() < inChannel.size());
		//System.out.println("last key = "+lastKey);
		//System.out.println("current key = "+currentKey);
		//System.out.println("file size = "+inChannel.size());
		this.updateList(inChannel.size());
		inChannel.position(0);	// reset to the beginning ...
		//System.out.println("indexList: "+indexList);
		return indexList;
	}
	
	private long makeSubDirs(long numOfChunks, Path outPath, boolean toDisc) {
		long numOfSubDirs = 0;
		if (numOfChunks > Nio.MAX_FILES_PER_DIR && toDisc) {
			numOfSubDirs = numOfChunks/Nio.MAX_FILES_PER_DIR + (numOfChunks%Nio.MAX_FILES_PER_DIR == 0 ? 0 : 1);
			//System.out.println("num of subdirs = "+numOfSubDirs);
			for (int i = 0; i < numOfSubDirs; i++) {
				createDir(outPath,""+i);
			}
		}
		return numOfSubDirs;
	}
	
	private Path setFileName(long numOfSubDirs, Path outPath, int i, String postfix) {
		if (postfix == null)
			return numOfSubDirs == 0 ? Paths.get(outPath.toString(), CHUNK_FILE_PREFIX+i) : Paths.get(outPath.toString(), (i/Nio.MAX_FILES_PER_DIR)+"/"+CHUNK_FILE_PREFIX+i);
		else
			return numOfSubDirs == 0 ? Paths.get(outPath.toString(), CHUNK_FILE_PREFIX+i+this.replaceNonSysChars(postfix)) : Paths.get(outPath.toString(), (i/Nio.MAX_FILES_PER_DIR)+"/"+CHUNK_FILE_PREFIX+i+this.replaceNonSysChars(postfix));
	}
	
	private String replaceNonSysChars (String str) {
		str=str.replace("<", "_");
		str=str.replace(">", "_");
		return str;
	}
	
	private void writeToDisc(FileChannel inChannel, long numOfChunks, long numOfSubDirs, Path outPath, long[] headerBodyTailSize, BufferHandlerInterface bhi) throws IOException {
		WritableByteChannel target = null;
		Path actPath = null;
		long pos = 0;
		for (int i = 0; i < numOfChunks; i++) {
			if (headerBodyTailSize != null) {
				actPath = this.setFileName(numOfSubDirs, outPath, i, null);
				target = Files.newByteChannel(actPath, EnumSet.of(StandardOpenOption.CREATE,StandardOpenOption.WRITE));
				if (i == 0) {
					inChannel.transferTo(pos,  headerBodyTailSize[0], target);
					bhi.handleBuffer(this,actPath.toAbsolutePath().toString());
					pos+=headerBodyTailSize[0];
					continue;
				}
				if (i == (numOfChunks-1)) {
					inChannel.transferTo(pos,  headerBodyTailSize[2], target);
					bhi.handleBuffer(this,actPath.toAbsolutePath().toString());
					pos+=headerBodyTailSize[2];
					continue;
				}
				inChannel.transferTo(pos,  headerBodyTailSize[1], target);
				bhi.handleBuffer(this,actPath.toAbsolutePath().toString());
				pos+=headerBodyTailSize[1];
				target = null;
			} else {
				//System.out.println("chunk len = "+this.getChunkLen(i));
				actPath = this.setFileName(numOfSubDirs, outPath, i, "_"+new String(this.getChunkPattern(i)));
				target = Files.newByteChannel(actPath, EnumSet.of(StandardOpenOption.CREATE,StandardOpenOption.WRITE));
				inChannel.transferTo(pos,this.getChunkLen(i), target);
				bhi.handleBuffer(this,actPath.toAbsolutePath().toString());
				pos+=this.getChunkLen(i);
			}
		}
	}
	
	private String[] prepOutputString(ByteBuffer[] bb) {
		String[] outList = (bb == null) ? new String[this.indexList == null ? 0 : this.indexList.size()] : new String[bb.length];
		if (outList.length == 0) {
			return outList;
		}
		int i = 0;
		if (bb == null) {
			ArrayList<Object> al = null;
			for (Long key : this.indexList.keySet()) {
				al = this.indexList.get(key);
				outList[i++] = ""+key.toString()+"|"+al.get(0).toString()+"|"+new String((byte[])al.get(1));
			}
		} else {
			for (ByteBuffer b : bb) {
				outList[i++] = new String(b.array());
			}
		}
		return outList;
	}
	
	public String[] splitt(FileChannel inChannel, Path outPath, boolean hasHeaderBodyTail, boolean isFixedLen, boolean toDisc, boolean useScattering, long[] headerBodyTailSize, long varLenBufferSize) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		ByteBuffer[] bufferList = null;
		BufferHandlerInterface bhi = (BufferHandlerInterface) Class.forName(BYTE_BUFFER_HANDLER_CLASSNAME).newInstance();
		int numOfChunks = -1;
		long numOfSubDirs;
		
		if (!isFixedLen) {
			this.determineChunks(inChannel, varLenBufferSize);
			numOfChunks = this.indexList.size();
			if (toDisc) {
				numOfSubDirs = this.makeSubDirs(numOfChunks, outPath, toDisc);
				this.writeToDisc(inChannel, numOfChunks, numOfSubDirs, outPath, null, bhi);
			}
			return this.prepOutputString(null);
		}
		
		if (hasHeaderBodyTail && useScattering)
			numOfChunks = (int) ((inChannel.size()-headerBodyTailSize[0]-headerBodyTailSize[2])/headerBodyTailSize[1])+2;
		else if (!hasHeaderBodyTail && useScattering)
			numOfChunks = (int) (inChannel.size()/headerBodyTailSize[1]);
		else if (!useScattering)
			numOfChunks = 1;
		//System.out.println("numOfChunks = "+numOfChunks);
		
		numOfSubDirs = this.makeSubDirs(numOfChunks, outPath, toDisc);
		
		if (!useScattering && !hasHeaderBodyTail && !toDisc) {
			bufferList = new ByteBuffer[(int) (numOfChunks)];	// should be 1
			bufferList[0] = ByteBuffer.allocate((int) headerBodyTailSize[1]);
			long bytesRead = 0;
			
			do {
				bytesRead = inChannel.read(bufferList[0]);
				if (bytesRead > 0) {
					bhi.handleBuffer(this,bufferList[0]);
					bufferList[0] = ByteBuffer.allocate((int) headerBodyTailSize[1]);
				}
			} while (bytesRead > 0);
			return null;
		}
		
		if (isFixedLen && hasHeaderBodyTail) {
			//System.out.println("num of buffers = "+numOfChunks);
			if (!toDisc) {
				bufferList = new ByteBuffer[(int) (numOfChunks)];
				for (int i = 0; i < (numOfChunks); i++) {
					if (i == 0) {					// header
						bufferList[i] = ByteBuffer.allocate((int) headerBodyTailSize[0]);
						//System.out.println("header capa = "+bufferList[i].capacity());
						//continue;
					}
					else if (i == (numOfChunks-1)) { // tail
						bufferList[i] = ByteBuffer.allocate((int) headerBodyTailSize[2]);
						//System.out.println("tail capa = "+bufferList[i].capacity());
						//continue;
					} else {						// body
						bufferList[i] = ByteBuffer.allocate((int) headerBodyTailSize[1]);
						//System.out.println("body capa = "+bufferList[i].capacity());
					}
				}
				//System.out.println("buffer list size = "+bufferList.length);
				inChannel.read(bufferList);	//scattering
				return this.prepOutputString(bufferList);
			} else {
				this.writeToDisc(inChannel, numOfChunks, numOfSubDirs, outPath, headerBodyTailSize, bhi);
			}
		}
		return this.prepOutputString(bufferList);
	}
	
	public static FileChannel prepFiles(String filename) throws IOException {
		SRC_FILENAME = filename;
		SRC_PATH = FS.getPath(SRC_FILENAME);
		System.out.println("SRC_PATH -> "+SRC_PATH.toAbsolutePath());
		FileChannel inChannel = FileChannel.open(SRC_PATH, StandardOpenOption.READ);
		//System.out.println("inChannel size = "+inChannel.size());
		return inChannel;
	}

	public static void main(String[] args) throws IOException, InterruptedException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		Nio nio = new Nio();
		
		//LinkedHashMap<byte[], byte[]> lhm = new LinkedHashMap<byte[],byte[]>();
		//lhm.put("C2".getBytes(), null);
		//lhm.put("A1".getBytes(), "\\".getBytes());
		//nio.setPatternList(lhm);
		nio.addPattern("<item>".getBytes(), "\\".getBytes());
		nio.addPattern("<xyz>".getBytes(), "\\".getBytes());
		
		/*
		Nio.SRC_PATH = FS.getPath(".");
	    System.out.println("SRC_PATH -> "+SRC_PATH.toAbsolutePath());
	    Path DEST_ROOT_PATH = FS.getPath("out");
	    System.out.println("DEST_ROOT_PATH -> "+DEST_ROOT_PATH.toAbsolutePath());
	    FileChannel inChannel = Nio.prepFiles("nio-data3.txt");
		boolean hasHeaderBodyTail = false; boolean isFixedLen = false; boolean toDisc = true; boolean useScattering = true; long varLenBufferSize = 200;
		String[] fileinfo = nio.splitt(inChannel, DEST_ROOT_PATH, hasHeaderBodyTail, isFixedLen, toDisc, useScattering, null, varLenBufferSize);
		for(String str : fileinfo) {
			System.out.println("str = "+str);
		}
		*/
		
		Nio.SRC_PATH = FS.getPath(".");
		System.out.println("SRC_PATH -> "+SRC_PATH.toAbsolutePath());
	    Path DEST_ROOT_PATH = FS.getPath("out");
	    System.out.println("DEST_ROOT_PATH -> "+DEST_ROOT_PATH.toAbsolutePath());
	    FileChannel inChannel = Nio.prepFiles("nio-data.txt");
		boolean hasHeaderBodyTail = true; boolean isFixedLen = true; boolean toDisc = false; boolean useScattering = true; long varLenBufferSize = -1L;
		String[] fileinfo = nio.splitt(inChannel, DEST_ROOT_PATH, hasHeaderBodyTail, isFixedLen, toDisc, useScattering, new long[]{128,256,128}, varLenBufferSize);
		for(String str : fileinfo) {
			System.out.println("str = "+str);
		}
		
		//boolean hasHeaderBodyTail = false; boolean isFixedLen = true; boolean toDisc = false; boolean useScattering = false; long varLenBufferSize = -1L;
		//ByteBuffer[] bbList = nio.splitt(inChannel, DEST_ROOT_PATH, hasHeaderBodyTail, isFixedLen, toDisc, useScattering, new long[]{128,256,128}, varLenBufferSize);
		
		inChannel.close();
	}

}
