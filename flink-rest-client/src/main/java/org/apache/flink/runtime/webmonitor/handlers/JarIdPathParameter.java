package org.apache.flink.runtime.webmonitor.handlers;


import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;

/**
 * Path parameter to identify uploaded jar files.
 */
public class JarIdPathParameter extends MessagePathParameter<String> {

  public static final String KEY = "jarid";

  protected JarIdPathParameter() {
    super(KEY);
  }

  @Override
  protected String convertFromString(final String value) throws ConversionException {
    final Path path = Paths.get(value);
    if (path.getParent() != null) {
      throw new ConversionException(String.format("%s must be a filename only (%s)", KEY, path));
    }
    return value;
  }

  @Override
  protected String convertToString(final String value) {
    return value;
  }

  @Override
  public String getDescription() {
    return "String value that identifies a jar. When uploading the jar a path is returned, where the filename "
        + "is the ID.";
  }
}

