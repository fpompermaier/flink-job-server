package org.apache.flink.client.program.rest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.retry.WaitStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServices;
import org.apache.flink.runtime.rest.FileUpload;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteMessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarRunMessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.JarRunRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarRunResponseBody;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadResponseBody;

public class RestClusterClientExtended<T> extends RestClusterClient<T> {

  /**
   * Returns the exceptions info bean of the failed job.
   * 
   * @param flinkJobId the failed Flink job Id
   * @return the exceptions info of the failed job
   * @throws InterruptedException if any other exception occurs
   * @throws ExecutionException if any other exception occurs
   */
  public JobExceptionsInfo getFlinkJobExceptionsInfo(JobID flinkJobId)
      throws InterruptedException, ExecutionException {
    final JobExceptionsMessageParameters params = new JobExceptionsMessageParameters();
    params.jobPathParameter.resolve(flinkJobId);
    return sendRequest(JobExceptionsHeaders.getInstance(), params, EmptyRequestBody.getInstance())
        .get();
  }

  /**
   * Upload a jar on the Flink cluster.
   * 
   * @param uploadedFile the file to upload
   * @return the JarUpload response
   * @throws InterruptedException if any exception occurs
   * @throws ExecutionException if any exception occurs
   * @throws IOException if any exception occurs during file creation
   */
  public JarUploadResponseBody uploadJar(File uploadedFile)
      throws InterruptedException, ExecutionException, IOException {
    final Collection<FileUpload> filesToUpload = new ArrayList<>(1);
    filesToUpload.add(new FileUpload(uploadedFile.toPath(), RestConstants.CONTENT_TYPE_JAR));
    return sendRetriableRequest(JarUploadHeaders.getInstance(),
        EmptyMessageParameters.getInstance(), EmptyRequestBody.getInstance(), filesToUpload,
        isConnectionProblemOrServiceUnavailable()).get();
  }


  /**
   * Delete the passed jar file.
   * 
   * @param jarFileName the jar file to delete.
   * @return the jar file to delete.
   * @throws InterruptedException if any exception occurs
   * @throws ExecutionException if any exception occurs
   */
  public EmptyResponseBody deleteJar(String jarFileName)
      throws InterruptedException, ExecutionException {
    final JarDeleteMessageParameters params = new JarDeleteMessageParameters();
    params.jarIdPathParameter.resolve(jarFileName);
    return sendRequest(JarDeleteHeaders.getInstance(), params, EmptyRequestBody.getInstance())
        .get();
  }

  /**
   * Run a flink job.
   * 
   * @param jarName the jar id
   * @param entryClassName the main class
   * @param programArguments the program arguments
   * @param parallelism the parallelism (could be null)
   * @param allowNonRestoredState if to restore state
   * @param savepointPath the savepoint path
   * @return the jar run info
   * @throws InterruptedException if any exceptions occurs
   * @throws ExecutionException if any exceptions occurs
   */
  public JarRunResponseBody runJob(String jarName, String entryClassName, String[] programArguments,
      Integer parallelism, Boolean allowNonRestoredState, String savepointPath)
      throws InterruptedException, ExecutionException {
    final JarRunRequestBody jarRunRequest = new JarRunRequestBody(//
        entryClassName, //
        null, // String.join(" ", programArguments), //
        Arrays.asList(programArguments), //
        parallelism, //
        null, //
        allowNonRestoredState != null ? allowNonRestoredState : null, //
        savepointPath != null ? savepointPath : null //
    );
    final JarRunMessageParameters params =
        JarRunHeaders.getInstance().getUnresolvedMessageParameters();
    params.jarIdPathParameter.resolve(jarName);
    final JarRunResponseBody ret =
        sendRequest(JarRunHeaders.getInstance(), params, jarRunRequest).get();
    return ret;
  }

  /**
   * Return true if the job is still running, false otherwise.
   * 
   * @param fjid the Flink Job ID
   * @return true if the job is still running, false otherwise.
   * @throws InterruptedException if any Exception occurs
   * @throws ExecutionException if any Exception occurs
   */
  public boolean isJobRunning(JobID fjid) throws InterruptedException, ExecutionException {
    return !getJobStatus(fjid).get().isTerminalState();
  }

  // Inherited constructors -------------------------------------------------

  public RestClusterClientExtended(Configuration configuration, RestClient restClient, T clusterId,
      WaitStrategy waitStrategy) throws Exception {
    super(configuration, restClient, clusterId, waitStrategy);
  }

  public RestClusterClientExtended(Configuration config, T clusterId,
      ClientHighAvailabilityServices clientHAServices) throws Exception {
    super(config, clusterId, clientHAServices);
  }

  public RestClusterClientExtended(Configuration config, T clusterId) throws Exception {
    super(config, clusterId);
  }

}
