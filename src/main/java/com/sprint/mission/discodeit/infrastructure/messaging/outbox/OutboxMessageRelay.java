package com.sprint.mission.discodeit.infrastructure.messaging.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxMessageRelay {

    private static final int BATCH_SIZE = 100;
    private static final int RETRY_BATCH_SIZE = 50;
    private static final long SEND_TIMEOUT_SECONDS = 5;
    private static final long RETRY_DELAY_MINUTES = 5;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 500)
    @SchedulerLock(
        name = "OutboxMessageRelay_publishPendingEvents",
        lockAtLeastFor = "PT0.3S",
        lockAtMostFor = "PT30S"
    )
    @Transactional
    public void publishPendingEvents() {
        Pageable pageable = PageRequest.of(0, BATCH_SIZE);
        List<OutboxEvent> events = outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(
            OutboxEventStatus.PENDING, pageable);

        if (events.isEmpty()) {
            return;
        }

        publishEventsAsync(events);
    }

    @Scheduled(fixedDelay = 60_000)
    @SchedulerLock(
        name = "OutboxMessageRelay_retryFailedEvents",
        lockAtLeastFor = "PT5S",
        lockAtMostFor = "PT60S"
    )
    @Transactional
    public void retryFailedEvents() {
        Instant retryThreshold = Instant.now().minusSeconds(RETRY_DELAY_MINUTES * 60);
        Pageable pageable = PageRequest.of(0, RETRY_BATCH_SIZE);

        List<OutboxEvent> failedEvents = outboxEventRepository.findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
            OutboxEventStatus.FAILED, retryThreshold, pageable);

        if (failedEvents.isEmpty()) {
            return;
        }

        log.info("Retrying {} failed outbox events", failedEvents.size());
        publishEventsAsync(failedEvents);
    }

    private void publishEventsAsync(List<OutboxEvent> events) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(events.size());

        for (OutboxEvent event : events) {
            CompletableFuture<Void> future = kafkaTemplate
                .send(event.getTopic(), event.getAggregateId().toString(), event.getPayload())
                .thenAccept(result -> {
                    event.markPublished();
                    log.debug("Published outbox event: [id={}, topic={}]", event.getId(), event.getTopic());
                })
                .exceptionally(ex -> {
                    event.markFailed();
                    log.error("Failed to publish outbox event: [id={}, topic={}]",
                        event.getId(), event.getTopic(), ex);
                    return null;
                });

            futures.add(future);
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(SEND_TIMEOUT_SECONDS * events.size() / 10 + SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Some outbox events may not have completed within timeout", e);
        }
    }
}
