package chatserver.validation;

import chatserver.model.MessageType;

import java.time.OffsetDateTime;
import java.util.regex.Pattern;

public class MessageValidator {

  private static final Pattern USERNAME_PATTERN = Pattern.compile("^[A-Za-z0-9]{3,20}$");

  public static void validateOrThrow(
      String roomId,
      String rawJsonUserId,
      String username,
      String message,
      String timestamp,
      String messageType
  ) {
    // roomId basic check (not required by screenshot but good practice)
    if (roomId == null || roomId.isBlank()) {
      throw new IllegalArgumentException("roomId is missing in path");
    }

    // userId: string but numeric range
    int uid;
    try {
      uid = Integer.parseInt(rawJsonUserId);
    } catch (Exception e) {
      throw new IllegalArgumentException("userId must be an integer string");
    }
    if (uid < 1 || uid > 100000) {
      throw new IllegalArgumentException("userId must be between 1 and 100000");
    }

    // username
    if (username == null || !USERNAME_PATTERN.matcher(username).matches()) {
      throw new IllegalArgumentException("username must be 3-20 alphanumeric characters");
    }

    // message length
    if (message == null || message.length() < 1 || message.length() > 500) {
      throw new IllegalArgumentException("message must be 1-500 characters");
    }

    // timestamp ISO-8601
    try {
      OffsetDateTime.parse(timestamp);
    } catch (Exception e) {
      throw new IllegalArgumentException("timestamp must be valid ISO-8601");
    }

    // messageType enum
    try {
      MessageType.valueOf(messageType);
    } catch (Exception e) {
      throw new IllegalArgumentException("messageType must be one of TEXT|JOIN|LEAVE");
    }
  }
}
