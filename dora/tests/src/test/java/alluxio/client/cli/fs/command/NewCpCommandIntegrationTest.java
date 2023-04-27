/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.cli.fs.FileSystemShell;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.client.file.FileInStream;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;

import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;

public class NewCpCommandIntegrationTest extends AbstractFileSystemShellTest {
  private static final String UFS_ROOT =
      AlluxioTestDirectory.createTemporaryDirectory("ufs_root").getAbsolutePath();
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @ClassRule
  public static LocalAlluxioClusterResource sResource = new LocalAlluxioClusterResource.Builder()
      .setProperty(PropertyKey.DORA_CLIENT_READ_LOCATION_POLICY_ENABLED, true)
      .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, UFS_ROOT)
      .setProperty(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false)
      .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false)
      .setProperty(PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES, Constants.KB)
      .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, "PAGE")
      .setProperty(PropertyKey.USER_NETTY_DATA_TRANSMISSION_ENABLED, true).build();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sLocalAlluxioCluster = sResource.get();
    sFileSystem = sLocalAlluxioCluster.getClient();
    sFsShell = new FileSystemShell(Configuration.global());
  }

  /**
   * Tests copying a file to a new location.
   */
  @Test
  public void copyFileNew() throws Exception {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    Assert.assertTrue(a.createNewFile());
    File b = new File(dstRoot, "b");
    Assert.assertFalse(b.exists());
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(a.getAbsolutePath(), buffer);
    sFsShell.run("cp", a.getAbsolutePath(), b.getAbsolutePath(), "--submit");
    CommonUtils.waitFor("file to be copied", () -> {
      assertEquals(0, sFsShell.run("cp", a.getAbsolutePath(), b.getAbsolutePath(), "--progress"));
      return mOutput.toString().contains("SUCCEED");
    });
    Assert.assertTrue(b.exists());
    try (InputStream in = Files.newInputStream(b.toPath())) {
      byte[] readBuffer = new byte[length];
      while (in.read(readBuffer) != -1) {
      }
      Assert.assertArrayEquals(buffer, readBuffer);
    }
  }

  /**
   * Tests copying a file to an existing directory.
   */
  @Test
  public void copyFileExisting() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell.run("cp", testDir + "/foobar4", testDir + "/bar");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testDir + "/bar/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI(testDir + "/bar/foobar4"), new AlluxioURI(testDir + "/foobar4")));
  }

  /**
   * Tests recursively copying a directory to a new location.
   */
  @Test
  public void copyDirNew() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret = sFsShell.run("cp", "-R", testDir, "/copy");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/bar")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/bar/foobar3")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foo")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foo/foobar1")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foo/foobar2")));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/copy/foobar4")));

    Assert.assertTrue(
        equals(new AlluxioURI("/copy/bar/foobar3"), new AlluxioURI(testDir + "/bar/foobar3")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foo/foobar1"), new AlluxioURI(testDir + "/foo/foobar1")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foo/foobar2"), new AlluxioURI(testDir + "/foo/foobar2")));
    Assert.assertTrue(
        equals(new AlluxioURI("/copy/foobar4"), new AlluxioURI(testDir + "/foobar4")));
  }

  private boolean equals(AlluxioURI file1, AlluxioURI file2) throws Exception {
    try (Closer closer = Closer.create()) {
      OpenFilePOptions openFileOptions =
          OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build();
      FileInStream is1 = closer.register(sFileSystem.openFile(file1, openFileOptions));
      FileInStream is2 = closer.register(sFileSystem.openFile(file2, openFileOptions));
      return IOUtils.contentEquals(is1, is2);
    }
  }
}
