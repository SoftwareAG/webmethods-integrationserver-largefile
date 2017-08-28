package tom;
import java.nio.*;

public interface BufferHandlerInterface {
	public void handleBuffer(Nio nio, ByteBuffer bb);
	public void handleBuffer(Nio nio, String refToBuffer);
}
