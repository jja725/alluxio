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

import alluxio.underfs.UfsStatus;

/**
 * Load data subtask.
 */
public class LoadDataSubTask extends LoadSubTask {
  private final long mVirtualBlockSize;

  LoadDataSubTask(UfsStatus ufsStatus, HashKey hashKey, long virtualBlockSize) {
    super(ufsStatus, hashKey);
    mVirtualBlockSize = virtualBlockSize;
  }

  @Override
  long getOffset() {
    return mVirtualBlockSize * mHashKey.getVirtualBlockIndex().getAsInt();
  }

  @Override
  public long getLength() {
    if (mVirtualBlockSize == 0) {
      return mUfsStatus.asUfsFileStatus().getContentLength();
    }
    long leftover = mUfsStatus.asUfsFileStatus().getContentLength() - getOffset();
    return Math.min(leftover, mVirtualBlockSize);
  }

  @Override
  boolean isLoadMetadata() {
    return false;
  }
}
