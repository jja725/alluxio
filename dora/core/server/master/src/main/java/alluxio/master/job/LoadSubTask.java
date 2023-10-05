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

import alluxio.grpc.LoadFailure;
import alluxio.underfs.UfsStatus;

import java.util.OptionalInt;

/**
 * Load sub task. It's either load metadata or load data.
 */
public abstract class LoadSubTask {
  protected UfsStatus mUfsStatus;
  protected HashKey mHashKey;

  LoadSubTask(UfsStatus ufsStatus, HashKey hashKey) {
    mUfsStatus = ufsStatus;
    mHashKey = hashKey;
  }

  /**
   * @return the offset
   */
  abstract long getOffset();

  /**
   * @return the length
   */
  abstract long getLength();

  /**
   * @return the ufs path
   */
  public String getUfsPath() {
    return mUfsStatus.getUfsFullPath().toString();
  }

  /**
   * @return whether it's load metadata task or it's load data task
   */
  abstract boolean isLoadMetadata();

  /**
   * @return the hash key string
   */
  public String getHashKeyString() {
    return mHashKey.toString();
  }

  /**
   * @param failure          the subtask failure from worker
   * @param virtualBlockSize the virtual block size
   * @return the subtask
   */
  public static LoadSubTask from(LoadFailure failure, long virtualBlockSize) {
    if (failure.hasUfsStatus()) {
      return new LoadMetadataSubTask(UfsStatus.fromProto(failure.getUfsStatus()),
          new HashKey(failure.getUfsStatus().getUfsFullPath(), OptionalInt.empty()));
    }
    else {
      UfsStatus status = UfsStatus.fromProto(failure.getBlock().getUfsStatus());
      int index;
      if (virtualBlockSize == 0) {
        index = 0;
      }
      else {
        index = (int) (failure.getBlock().getOffsetInFile() / virtualBlockSize);
      }
      return new LoadDataSubTask(status,
          new HashKey(status.getUfsFullPath().toString(), OptionalInt.of(index)), virtualBlockSize);
    }
  }
}
