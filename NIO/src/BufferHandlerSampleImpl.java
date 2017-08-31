
import java.nio.ByteBuffer;

import tom.Nio;


public class BufferHandlerSampleImpl implements tom.BufferHandlerInterface {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleBuffer(Nio nio, ByteBuffer bb) {
		// TODO Auto-generated method stub
		System.out.println("*** start handle byte buffer here! ***");
		nio.printBuffer(bb);
		System.out.println();
		long pos = nio.search(bb.array(), 0, bb.array().length, "1a".getBytes());
		System.out.println("pos of '1a' is "+pos);
		if (pos > 0) {
			nio.storeMetaInfo("key1", bb, (int) pos, "1a".length());
		}
		System.out.println("*** end handle byte buffer here! ***");
	}

	@Override
	public void handleBuffer(Nio nio, String refToBuffer) {
		// TODO Auto-generated method stub
		System.out.println("buffer location = "+refToBuffer);
	}

}
