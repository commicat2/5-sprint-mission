package com.sprint.mission.discodeit.binarycontent.domain.event;

import java.util.UUID;

public record BinaryContentStorageFailedEvent(
    UUID binaryContentId,
    String errorMessage,
    String requestId
) {
    public static final String TOPIC = "discodeit.binary-content.storage-failed";
}
