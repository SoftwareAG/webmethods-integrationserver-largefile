package tom;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Observable;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.eaio.stringsearch.BoyerMooreHorspool;
import com.eaio.stringsearch.BoyerMooreSunday;

public class Nio extends Observable {
	
	public static FileSystem FS = FileSystems.getDefault();
	public static Path SRC_PATH;
	public static String SRC_FILENAME;
	public static Path DEST_ROOT_PATH;
	public static String CACHE_NAME="NioCache";
	public static String CACHE_MGR_CONFIG="./ehcache.xml";
	public static long MAX_FILES_PER_DIR = 2;
	public static String CHUNK_FILE_PREFIX="chunk";
	public static String BYTE_BUFFER_HANDLER_CLASSNAME = "BufferHandlerSampleImpl";	// testing purpose only!!!
	final CacheManager cacheManager = CacheManager.newInstance(CACHE_MGR_CONFIG);
	final Cache cache = cacheManager.getCache(CACHE_NAME);
	
	
	public WritableByteChannel checkAndCreate(Path path, int dir, String filename) {
		Path retPath = dir != -1 ? Paths.get(path.toString(), ""+dir):path;
		WritableByteChannel wbc = null;
		if (Files.notExists(retPath, LinkOption.NOFOLLOW_LINKS) && dir != -1) {
			System.out.println(path.toAbsolutePath()+" doesn't exists.");
			try {
				Files.createDirectory(retPath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				return null;
			}
		}
		retPath = Paths.get(retPath.toString(),filename);
		if (Files.notExists(retPath, LinkOption.NOFOLLOW_LINKS)) {
			System.out.println(path.toAbsolutePath()+" doesn't exists.");
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
		System.out.println("cap = "+buffer.capacity());
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
	
	public int search (ByteBuffer bb, byte[] pattern) {
		BoyerMooreSunday bms = new BoyerMooreSunday();
		//long start1 = System.nanoTime();
		Object obj = bms.processBytes(pattern);
		int pos = bms.searchBytes(bb.array(), 0, bb.capacity(), pattern, obj);
		//long end1 = System.nanoTime();
		//System.out.println(pos+" - duration: "+(end1-start1)+" - "+bb.position());
		return pos;
	}
	
	public void storeMetaInfo(String key, ByteBuffer bb, int offset, int len) {
		byte[] dst = new byte[len];
		bb.position(offset);
		bb.get(dst, 0, len);
		//System.out.println("-->"+new String(dst));
		System.out.println("cache == "+cache);
		cache.put(new Element(key,dst));
	}
	
	public byte[] getMetaInfo(String key) {
		Element meta = cache.get(key);
		return (byte[]) meta.getObjectValue();
	}
	
	public boolean deleteMetaInfo (String key) {
		return cache.remove(key);
	}
	
	public void createDir(Path outPath, String dir) {
		Path tmpPath = Paths.get(outPath.toString(), dir);
		try {
			Files.createDirectory(tmpPath);
		} catch (IOException e) {
			System.out.println("ignoring exception "+e);
		}
	}
	
	
	public ByteBuffer[] splitt(FileChannel inChannel, Path outPath, boolean hasHeaderBodyTail, boolean isFixedLen, boolean toDisc, boolean useScattering, long[] headerBodyTailSize) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		ByteBuffer[] bufferList = null;
		BufferHandlerInterface bhi = (BufferHandlerInterface) Class.forName(BYTE_BUFFER_HANDLER_CLASSNAME).newInstance();
		int numOfChunks = -1;
		
		if (hasHeaderBodyTail && useScattering)
			numOfChunks = (int) ((inChannel.size()-headerBodyTailSize[0]-headerBodyTailSize[2])/headerBodyTailSize[1])+2;
		else if (!hasHeaderBodyTail && useScattering)
			numOfChunks = (int) (inChannel.size()/headerBodyTailSize[1]);
		else if (!useScattering)
			numOfChunks = 1;
		System.out.println("numOfChunks = "+numOfChunks);
		long numOfSubDirs = 0;
		if (numOfChunks > Nio.MAX_FILES_PER_DIR && toDisc) {
			numOfSubDirs = numOfChunks/Nio.MAX_FILES_PER_DIR + (numOfChunks%Nio.MAX_FILES_PER_DIR == 0 ? 0 : 1);
			System.out.println("num of subdirs = "+numOfSubDirs);
			for (int i = 0; i < numOfSubDirs; i++) {
				createDir(outPath,""+i);
			}
		}
		
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
			System.out.println("num of buffers = "+numOfChunks);
			if (!toDisc) {
				bufferList = new ByteBuffer[(int) (numOfChunks)];
				for (int i = 0; i < (numOfChunks); i++) {
					if (i == 0) {					// header
						bufferList[i] = ByteBuffer.allocate((int) headerBodyTailSize[0]);
						System.out.println("header capa = "+bufferList[i].capacity());
						//continue;
					}
					else if (i == (numOfChunks-1)) { // tail
						bufferList[i] = ByteBuffer.allocate((int) headerBodyTailSize[2]);
						System.out.println("tail capa = "+bufferList[i].capacity());
						//continue;
					} else {						// body
						bufferList[i] = ByteBuffer.allocate((int) headerBodyTailSize[1]);
						System.out.println("body capa = "+bufferList[i].capacity());
					}
				}
				System.out.println("buffer list size = "+bufferList.length);
				inChannel.read(bufferList);	//scattering
				return bufferList;
			} else {
				WritableByteChannel target = null;
				Path actPath = null;
				long pos = 0;
				for (int i = 0; i < numOfChunks; i++) {
					actPath = numOfSubDirs == 0 ? Paths.get(outPath.toString(), CHUNK_FILE_PREFIX+i) : Paths.get(outPath.toString(), (i/Nio.MAX_FILES_PER_DIR)+"/"+CHUNK_FILE_PREFIX+i);
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
				}
			}
		}
		return bufferList;
	}

	public static void main(String[] args) throws IOException, InterruptedException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		Nio nio = new Nio();
		
	    
	    SRC_FILENAME = "nio-data.txt";
		//SRC_FILENAME = "nio-data2.txt";
	    Path SRC_PATH = FS.getPath(SRC_FILENAME);
	    System.out.println("SRC_PATH -> "+SRC_PATH.toAbsolutePath());
	    Path DEST_ROOT_PATH = FS.getPath("out");
	    System.out.println("DEST_ROOT_PATH -> "+DEST_ROOT_PATH.toAbsolutePath());
	    long start1, end1, start2, end2;
		
		FileChannel inChannel = FileChannel.open(SRC_PATH, StandardOpenOption.READ);
		System.out.println("inChannel size = "+inChannel.size());
		WritableByteChannel outChannel = null;	// = Files.newByteChannel(outPath, EnumSet.of(StandardOpenOption.CREATE,StandardOpenOption.APPEND));

		boolean hasHeaderBodyTail = true; boolean isFixedLen = true; boolean toDisc = true; boolean useScattering = true;
		ByteBuffer[] bbList = nio.splitt(inChannel, DEST_ROOT_PATH, hasHeaderBodyTail, isFixedLen, toDisc, useScattering, new long[]{128,256,128});
		
		//boolean hasHeaderBodyTail = false; boolean isFixedLen = true; boolean toDisc = false; boolean useScattering = false;
		//ByteBuffer[] bbList = nio.splitt(inChannel, DEST_ROOT_PATH, hasHeaderBodyTail, isFixedLen, toDisc, useScattering, new long[]{128,256,128});
		
		inChannel.close();
	}

}
