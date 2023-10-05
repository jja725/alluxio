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

package alluxio.master.job;

import static java.lang.String.format;

import java.util.OptionalInt;

/**
 * The key for accessing the hash ring.
 * There's two types of usage for this key:
 * 1. metadata operation: the partition index is not present
 * 2. data operation: the partition index is present and decided based on the
 *   virtual block index.
 *
 * Notice for compatibility reason, when the partition index is 0
 * or not present(meaning it's a metadata operation), we don't append the virtual block
 * index to the key.
 *
 */
public final class HashKey {
  private String mUFSPath;
  private final OptionalInt mVirtualBlockIndex;

  /**
   * @return the partition index of the key
   */
  public OptionalInt getVirtualBlockIndex() {
    return mVirtualBlockIndex;
  }

  /**
   * constructor for HashKey.
   *
   * @param ufsPath           the ufs path
   * @param virtualBlockIndex the virtual block index
   */
  public HashKey(String ufsPath, OptionalInt virtualBlockIndex) {
    mUFSPath = ufsPath;
    mVirtualBlockIndex = virtualBlockIndex;
  }

  @Override
  public String toString() {
    if (mVirtualBlockIndex.isPresent() && mVirtualBlockIndex.getAsInt() != 0) {
      // use the same format as ConsistentHashProvider,
      // we can't distinguish the two index, but it's fine
      return format("%s%d", mUFSPath, mVirtualBlockIndex.getAsInt());
    }
    else {
      return mUFSPath;
    }
  }
}
