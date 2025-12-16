package com.sprint.mission.discodeit.infrastructure.messaging.outbox;

import com.sprint.mission.discodeit.common.infrastructure.outbox.AggregateType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
@DisplayName("OutboxMessageRelay 단위 테스트")
class OutboxMessageRelayTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @InjectMocks
    private OutboxMessageRelay outboxMessageRelay;

    private static final int BATCH_SIZE = 100;
    private static final int RETRY_BATCH_SIZE = 50;

    @Nested
    @DisplayName("publishPendingEvents")
    class PublishPendingEvents {

        @Test
        @DisplayName("대기 중인 이벤트가 없으면 아무것도 발행하지 않음")
        void publishPendingEvents_noEvents_doesNothing() {
            // given
            Pageable pageable = PageRequest.of(0, BATCH_SIZE);
            given(outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING, pageable))
                .willReturn(Collections.emptyList());

            // when
            outboxMessageRelay.publishPendingEvents();

            // then
            then(kafkaTemplate).should(never()).send(any(), any(), any());
        }

        @Test
        @DisplayName("이벤트 발행 성공 시 PUBLISHED로 상태 변경")
        void publishPendingEvents_success_marksPublished() {
            // given
            UUID aggregateId = UUID.randomUUID();
            String topic = "test.topic";
            String payload = "{\"data\":\"test\"}";

            OutboxEvent event = createOutboxEvent(UUID.randomUUID(), AggregateType.USER, aggregateId, topic, payload);

            Pageable pageable = PageRequest.of(0, BATCH_SIZE);
            given(outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING, pageable))
                .willReturn(List.of(event));

            CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
            given(kafkaTemplate.send(topic, aggregateId.toString(), payload)).willReturn(future);

            // when
            outboxMessageRelay.publishPendingEvents();

            // then
            then(kafkaTemplate).should().send(topic, aggregateId.toString(), payload);
            assertThat(event.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
        }

        @Test
        @DisplayName("다수의 이벤트를 병렬로 발행")
        void publishPendingEvents_multipleEvents_publishesInParallel() {
            // given
            OutboxEvent event1 = createOutboxEvent(
                UUID.randomUUID(), AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}");
            OutboxEvent event2 = createOutboxEvent(
                UUID.randomUUID(), AggregateType.CHANNEL, UUID.randomUUID(), "topic2", "{\"data\":\"2\"}");
            OutboxEvent event3 = createOutboxEvent(
                UUID.randomUUID(), AggregateType.MESSAGE, UUID.randomUUID(), "topic3", "{\"data\":\"3\"}");

            Pageable pageable = PageRequest.of(0, BATCH_SIZE);
            given(outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING, pageable))
                .willReturn(List.of(event1, event2, event3));

            CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
            given(kafkaTemplate.send(any(), any(), any())).willReturn(future);

            // when
            outboxMessageRelay.publishPendingEvents();

            // then
            then(kafkaTemplate).should(times(3)).send(any(), any(), any());
            assertThat(event1.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
            assertThat(event2.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
            assertThat(event3.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
        }

        @Test
        @DisplayName("Kafka 전송 실패 시 FAILED로 상태 변경하고 나머지 계속 처리")
        void publishPendingEvents_partialFailure_marksFailedAndContinues() {
            // given
            OutboxEvent event1 = createOutboxEvent(
                UUID.randomUUID(), AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}");
            OutboxEvent event2 = createOutboxEvent(
                UUID.randomUUID(), AggregateType.CHANNEL, UUID.randomUUID(), "topic2", "{\"data\":\"2\"}");

            Pageable pageable = PageRequest.of(0, BATCH_SIZE);
            given(outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(OutboxEventStatus.PENDING, pageable))
                .willReturn(List.of(event1, event2));

            CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new RuntimeException("Kafka error"));

            CompletableFuture<SendResult<String, String>> successFuture = CompletableFuture.completedFuture(null);

            given(kafkaTemplate.send(eq("topic1"), any(), any())).willReturn(failedFuture);
            given(kafkaTemplate.send(eq("topic2"), any(), any())).willReturn(successFuture);

            // when
            outboxMessageRelay.publishPendingEvents();

            // then
            assertThat(event1.getStatus()).isEqualTo(OutboxEventStatus.FAILED);
            assertThat(event2.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
        }
    }

    @Nested
    @DisplayName("retryFailedEvents")
    class RetryFailedEvents {

        @Test
        @DisplayName("실패한 이벤트가 없으면 아무것도 처리하지 않음")
        void retryFailedEvents_noFailedEvents_doesNothing() {
            // given
            given(outboxEventRepository.findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                eq(OutboxEventStatus.FAILED), any(Instant.class), any(Pageable.class)))
                .willReturn(Collections.emptyList());

            // when
            outboxMessageRelay.retryFailedEvents();

            // then
            then(kafkaTemplate).should(never()).send(any(), any(), any());
        }

        @Test
        @DisplayName("실패한 이벤트 재시도 성공 시 PUBLISHED로 상태 변경")
        void retryFailedEvents_success_marksPublished() {
            // given
            OutboxEvent failedEvent = createOutboxEvent(
                UUID.randomUUID(), AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}");
            failedEvent.markFailed();

            given(outboxEventRepository.findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                eq(OutboxEventStatus.FAILED), any(Instant.class), any(Pageable.class)))
                .willReturn(List.of(failedEvent));

            CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
            given(kafkaTemplate.send(any(), any(), any())).willReturn(future);

            // when
            outboxMessageRelay.retryFailedEvents();

            // then
            then(kafkaTemplate).should().send(eq("topic1"), any(), any());
            assertThat(failedEvent.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
        }

        @Test
        @DisplayName("실패한 이벤트 재시도 실패 시 FAILED 상태 유지")
        void retryFailedEvents_retryFails_remainsFailed() {
            // given
            OutboxEvent failedEvent = createOutboxEvent(
                UUID.randomUUID(), AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}");
            failedEvent.markFailed();

            given(outboxEventRepository.findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                eq(OutboxEventStatus.FAILED), any(Instant.class), any(Pageable.class)))
                .willReturn(List.of(failedEvent));

            CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new RuntimeException("Still failing"));
            given(kafkaTemplate.send(any(), any(), any())).willReturn(failedFuture);

            // when
            outboxMessageRelay.retryFailedEvents();

            // then
            assertThat(failedEvent.getStatus()).isEqualTo(OutboxEventStatus.FAILED);
        }
    }

    private OutboxEvent createOutboxEvent(
        UUID id, AggregateType aggregateType, UUID aggregateId, String topic, String payload
    ) {
        OutboxEvent event = new OutboxEvent(aggregateType, aggregateId, topic, payload);
        ReflectionTestUtils.setField(event, "id", id);
        return event;
    }
}
