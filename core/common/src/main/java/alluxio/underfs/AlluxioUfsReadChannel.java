package alluxio.underfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * A read only {@link SeekableByteChannel} for Alluxio UFS.
 */
public class AlluxioUfsReadChannel implements SeekableByteChannel {
  private boolean mIsOpen;
  private final InputStream mInputStream;

  /**
   * Constructs a new {@link AlluxioUfsReadChannel}.
   *
   * @param inputStream the input stream to read from
   */
  public AlluxioUfsReadChannel(InputStream inputStream) {
    mInputStream = inputStream;
    mIsOpen = true;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return 0;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return 0;
  }

  @Override
  public long position() throws IOException {
    return 0;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    return null;
  }

  @Override
  public long size() throws IOException {
    return 0;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public boolean isOpen() {
    return mChannelIsOpen;
  }

  @Override
  public void close() throws IOException {

  }
}
