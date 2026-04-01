package com.sprint.mission.discodeit.infrastructure.messaging.outbox;

public enum OutboxEventStatus {
    PENDING,
    PUBLISHED,
    FAILED
}
