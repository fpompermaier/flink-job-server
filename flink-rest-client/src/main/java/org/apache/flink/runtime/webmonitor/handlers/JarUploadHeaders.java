package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * {@link MessageHeaders} for uploading jars.
 */
public final class JarUploadHeaders
    implements MessageHeaders<EmptyRequestBody, JarUploadResponseBody, EmptyMessageParameters> {

  public static final String URL = "/jars/upload";
  private static final JarUploadHeaders INSTANCE = new JarUploadHeaders();

  private JarUploadHeaders() {}

  @Override
  public Class<JarUploadResponseBody> getResponseClass() {
    return JarUploadResponseBody.class;
  }

  @Override
  public HttpResponseStatus getResponseStatusCode() {
    return HttpResponseStatus.OK;
  }

  @Override
  public Class<EmptyRequestBody> getRequestClass() {
    return EmptyRequestBody.class;
  }

  @Override
  public EmptyMessageParameters getUnresolvedMessageParameters() {
    return EmptyMessageParameters.getInstance();
  }

  @Override
  public HttpMethodWrapper getHttpMethod() {
    return HttpMethodWrapper.POST;
  }

  @Override
  public String getTargetRestEndpointURL() {
    return URL;
  }

  public static JarUploadHeaders getInstance() {
    return INSTANCE;
  }

  @Override
  public String getDescription() {
    return "Uploads a jar to the cluster. The jar must be sent as multi-part data. Make sure that the \"Content-Type\""
        + " header is set to \"application/x-java-archive\", as some http libraries do not add the header by default.\n"
        + "Using 'curl' you can upload a jar via 'curl -X POST -H \"Expect:\" -F \"jarfile=@path/to/flink-job.jar\" http://hostname:port"
        + URL + "'.";
  }

  @Override
  public boolean acceptsFileUploads() {
    return true;
  }
}