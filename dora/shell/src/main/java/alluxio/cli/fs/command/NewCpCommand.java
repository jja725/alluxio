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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.CopyJobPOptions;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.job.CopyJobRequest;
import alluxio.job.JobDescription;
import alluxio.resource.CloseableResource;
import alluxio.util.FormatUtils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies a file or a directory in the Alluxio filesystem using job service.
 */
@ThreadSafe
@PublicApi
public final class NewCpCommand extends AbstractFileSystemCommand {
  private static final JobProgressReportFormat DEFAULT_FORMAT = JobProgressReportFormat.TEXT;
  private static final String JOB_TYPE = "copy";

  private final CpCommand mCpCommand;

  private static final Option RECURSIVE_OPTION =
      Option.builder("R").longOpt("recursive")
          .required(false)
          .hasArg(false)
          .desc("copy files in subdirectories recursively")
          .build();
  private static final Option RECURSIVE_ALIAS_OPTION =
      Option.builder("r")
          .required(false)
          .hasArg(false)
          .desc("copy files in subdirectories recursively")
          .build();
  public static final Option THREAD_OPTION =
      Option.builder()
          .longOpt("thread")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("threads")
          .type(Number.class)
          .desc("Number of threads used to copy files in parallel, default value is CPU cores * 2")
          .build();
  private static final Option PRESERVE_OPTION =
      Option.builder("p")
          .longOpt("preserve")
          .required(false)
          .desc("Preserve file permission attributes when copying files. "
              + "All ownership, permissions and ACLs will be preserved")
          .build();
  private static final Option SUBMIT_OPTION =
      Option.builder()
          .longOpt("submit")
          .required(false)
          .hasArg(false)
          .desc("Submit copy job to Alluxio master, update job options if already exists.")
          .build();

  private static final Option STOP_OPTION =
      Option.builder()
          .longOpt("stop")
          .required(false)
          .hasArg(false)
          .desc("Stop a copy job if it's still running.")
          .build();

  private static final Option PROGRESS_OPTION =
      Option.builder()
          .longOpt("progress")
          .required(false)
          .hasArg(false)
          .desc("Get progress report of a copy job.")
          .build();

  private static final Option PARTIAL_LISTING_OPTION =
      Option.builder()
          .longOpt("partial-listing")
          .required(false)
          .hasArg(false)
          .desc("Use partial directory listing. This limits the memory usage "
              + "and starts copy sooner for larger directory. But progress "
              + "report cannot report on the total number of files because the "
              + "whole directory is not listed yet.")
          .build();

  private static final Option VERIFY_OPTION =
      Option.builder()
          .longOpt("verify")
          .required(false)
          .hasArg(false)
          .desc("Run verification when copy finish and copy new files if any.")
          .build();

  private static final Option OVERWRITE_OPTION =
      Option.builder()
          .longOpt("overwrite")
          .required(false)
          .hasArg(false)
          .desc("Overwrites the files if the files with the same "
              + "name are existed on the target side")
          .build();

  private static final Option BANDWIDTH_OPTION =
      Option.builder()
          .longOpt("bandwidth")
          .required(false)
          .hasArg(true)
          .desc("Single worker read bandwidth limit.")
          .build();

  private static final Option PROGRESS_FORMAT =
      Option.builder()
          .longOpt("format")
          .required(false)
          .hasArg(true)
          .desc("Format of the progress report, supports TEXT and JSON. If not "
              + "set, TEXT is used.")
          .build();

  private static final Option PROGRESS_VERBOSE =
      Option.builder()
          .longOpt("verbose")
          .required(false)
          .hasArg(false)
          .desc("Whether to return a verbose progress report with detailed errors")
          .build();

  private static final Option CHECK_CONTENT =
      Option.builder()
          .longOpt("verbose")
          .required(false)
          .hasArg(false)
          .desc("Whether to check content hash when copying files")
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public NewCpCommand(FileSystemContext fsContext) {
    super(fsContext);
    mCpCommand = new CpCommand(fsContext);
  }

  @Override
  public String getCommandName() {
    return "cp";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    if (isNewFormat(cl)) {
      CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
      int commands = 0;
      if (cl.hasOption(SUBMIT_OPTION.getLongOpt())) {
        commands++;
      }
      if (cl.hasOption(STOP_OPTION.getLongOpt())) {
        commands++;
      }
      if (cl.hasOption(PROGRESS_OPTION.getLongOpt())) {
        commands++;
      }
      if (commands != 1) {
        throw new InvalidArgumentException("Must have one of submit / stop / progress");
      }
    } else {
      mCpCommand.validateArgs(cl);
    }
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION)
        .addOption(RECURSIVE_ALIAS_OPTION)
        .addOption(THREAD_OPTION)
        .addOption(BANDWIDTH_OPTION)
        .addOption(PARTIAL_LISTING_OPTION)
        .addOption(VERIFY_OPTION)
        .addOption(SUBMIT_OPTION)
        .addOption(STOP_OPTION)
        .addOption(OVERWRITE_OPTION)
        .addOption(PROGRESS_OPTION)
        .addOption(PROGRESS_FORMAT)
        .addOption(PROGRESS_VERBOSE)
        .addOption(PRESERVE_OPTION)
        .addOption(CHECK_CONTENT);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    AlluxioURI dstPath = new AlluxioURI(args[1]);
    if (!isNewFormat(cl)) {
      return mCpCommand.run(cl);
    }
    if (srcPath.containsWildcard() || dstPath.containsWildcard()) {
      throw new UnsupportedOperationException("Copy does not support wildcard path");
    }

    if (cl.hasOption(SUBMIT_OPTION.getLongOpt())) {
      OptionalLong bandwidth = OptionalLong.empty();
      if (cl.hasOption(BANDWIDTH_OPTION.getLongOpt())) {
        bandwidth = OptionalLong.of(FormatUtils.parseSpaceSize(
            cl.getOptionValue(BANDWIDTH_OPTION.getLongOpt())));
      }
      return submitCopy(
          srcPath,
          dstPath,
          bandwidth,
          cl.hasOption(PARTIAL_LISTING_OPTION.getLongOpt()),
          cl.hasOption(VERIFY_OPTION.getLongOpt()),
          cl.hasOption(OVERWRITE_OPTION.getLongOpt()),
          cl.hasOption(CHECK_CONTENT.getLongOpt())
      );
    }

    if (cl.hasOption(STOP_OPTION.getLongOpt())) {
      return stopCopy(srcPath, dstPath);
    }
    JobProgressReportFormat format = DEFAULT_FORMAT;
    if (cl.hasOption(PROGRESS_OPTION.getLongOpt())) {
      if (cl.hasOption(PROGRESS_FORMAT.getLongOpt())) {
        format = JobProgressReportFormat.valueOf(cl.getOptionValue(PROGRESS_FORMAT.getLongOpt()));
      }
      return getProgress(srcPath, dstPath, format, cl.hasOption(PROGRESS_VERBOSE.getLongOpt()));
    }
    return 0;
  }

  private int submitCopy(AlluxioURI srcPath, AlluxioURI dstPath, OptionalLong bandwidth,
      boolean usePartialListing, boolean verify, boolean overwrite, boolean checkContent) {
    CopyJobPOptions.Builder options =
        alluxio.grpc.CopyJobPOptions.newBuilder().setPartialListing(usePartialListing)
                                    .setVerify(verify).setOverwrite(overwrite)
                                    .setCheckContent(checkContent);
    if (bandwidth.isPresent()) {
      options.setBandwidth(bandwidth.getAsLong());
    }
    CopyJobRequest job = new CopyJobRequest(srcPath.toString(), dstPath.toString(), options.build());
    try (
        CloseableResource<FileSystemMasterClient> client = mFsContext.acquireMasterClientResource())
    {
      Optional<String> jobId = client.get().submitJob(job);
      if (jobId.isPresent()) {
        System.out.printf("Copy from '%s' to '%s' is successfully submitted. JobId: %s%n", srcPath,
            dstPath, jobId.get());
      }
      else {
        System.out.printf("Copy already running from path '%s' to '%s', updated the job with "
                + "new bandwidth: %s, verify: %s%n", srcPath, dstPath,
            bandwidth.isPresent() ? String.valueOf(bandwidth.getAsLong()) : "unlimited", verify);
      }
      return 0;
    } catch (StatusRuntimeException e) {
      System.out.println(
          "Failed to submit copy job from " + srcPath + " to " + dstPath + ": " + e.getMessage());
      return -1;
    }
  }

  private int stopCopy(AlluxioURI srcPath, AlluxioURI dstPath) {
    try (
        CloseableResource<FileSystemMasterClient> client = mFsContext.acquireMasterClientResource())
    {
      if (client.get().stopJob(
          JobDescription.newBuilder().setPath(srcPath.toString() + ":" + dstPath.toString())
                        .setType(JOB_TYPE).build())) {
        System.out.printf("Copy job from '%s' to '%s' is successfully stopped.%n", srcPath,
            dstPath);
      }
      else {
        System.out.printf("Cannot find copy job from path %s to %s, it might have already been "
            + "stopped or finished%n", srcPath, dstPath);
      }
      return 0;
    } catch (StatusRuntimeException e) {
      System.out.println(
          "Failed to stop copy job from " + srcPath + " to " + dstPath + ": " + e.getMessage());
      return -1;
    }
  }

  private int getProgress(AlluxioURI srcPath, AlluxioURI dstPath, JobProgressReportFormat format,
      boolean verbose) {
    try (
        CloseableResource<FileSystemMasterClient> client = mFsContext.acquireMasterClientResource())
    {
      System.out.println("Progress for copying path '" + srcPath + "' to '" + dstPath + "':");
      System.out.println(client.get().getJobProgress(
          JobDescription.newBuilder().setPath(srcPath.toString() + ":" + dstPath.toString())
                        .setType(JOB_TYPE).build(), format, verbose));
      return 0;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        System.out.println("Copy for path '" + srcPath + "' to '" + dstPath + "' cannot be found.");
        return -2;
      }
      System.out.println(
          "Failed to get progress for copy job from " + srcPath + " to " + dstPath + ": "
              + e.getMessage());
      return -1;
    }
  }

  @Override
  public String getUsage() {
    return "For backward compatibility: cp [-R/-r/--recursive] [--buffersize <bytes>] <src> <dst>\n"
        + "For distributed copy:\n"
        + "\tcp <src> <dst> --submit [--bandwidth N] [--verify] [--partial-listing] [--overwrite]\n"
        + "\tcp <src> <dst> --stop\n"
        + "\tcp <src> <dst> --progress [--format TEXT|JSON] [--verbose]\n";
  }

  @Override
  public String getDescription() {
    return "Copies a file or a directory in the Alluxio filesystem or between local filesystem "
        + "and Alluxio filesystem. The -R/-r/--recursive flags are needed to copy"
        + "directories in the Alluxio filesystem. Local Path with schema \"file\"."
        + "The --submit flag is needed to submit a copy job. The --stop flag is needed to "
        + "stop a copy job. The --progress flag is needed to track the progress of a copy job.";
  }

  private boolean isNewFormat(CommandLine cl) {
    return cl.hasOption(STOP_OPTION.getLongOpt())
        || cl.hasOption(PROGRESS_OPTION.getLongOpt())
        || cl.hasOption(SUBMIT_OPTION.getLongOpt());
  }

  public static void main(String[] args) {
    AlluxioURI uri = new AlluxioURI("s3://overmind-test-data/load/small/dir-99/");

    System.out.println(uri);

  }
}
