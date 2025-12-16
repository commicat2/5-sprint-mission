package com.sprint.mission.discodeit.infrastructure.messaging.outbox;

import com.sprint.mission.discodeit.common.infrastructure.outbox.AggregateType;
import com.sprint.mission.discodeit.global.config.JpaConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@Import(JpaConfig.class)
@DisplayName("OutboxEventRepository 슬라이스 테스트")
@ActiveProfiles("test")
class OutboxEventRepositoryTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void setUp() {
        outboxEventRepository.deleteAll();
    }

    @Nested
    @DisplayName("findAllByStatusOrderByCreatedAtAsc")
    class FindAllByStatusOrderByCreatedAtAsc {

        @Test
        @DisplayName("PENDING 상태의 이벤트만 조회")
        void findAllByStatusOrderByCreatedAtAsc_pendingOnly_returnsPendingEvents() {
            // given
            OutboxEvent pendingEvent1 = outboxEventRepository.save(
                new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}"));
            OutboxEvent pendingEvent2 = outboxEventRepository.save(
                new OutboxEvent(AggregateType.CHANNEL, UUID.randomUUID(), "topic2", "{\"data\":\"2\"}"));

            OutboxEvent publishedEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.MESSAGE, UUID.randomUUID(), "topic3", "{\"data\":\"3\"}"));
            publishedEvent.markPublished();
            outboxEventRepository.save(publishedEvent);

            OutboxEvent failedEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic4", "{\"data\":\"4\"}"));
            failedEvent.markFailed();
            outboxEventRepository.save(failedEvent);

            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(
                OutboxEventStatus.PENDING, pageable);

            // then
            assertThat(result).hasSize(2);
            assertThat(result).extracting(OutboxEvent::getStatus)
                .containsOnly(OutboxEventStatus.PENDING);
            assertThat(result).extracting(OutboxEvent::getId)
                .containsExactly(pendingEvent1.getId(), pendingEvent2.getId());
        }

        @Test
        @DisplayName("PUBLISHED 상태의 이벤트만 조회")
        void findAllByStatusOrderByCreatedAtAsc_publishedOnly_returnsPublishedEvents() {
            // given
            OutboxEvent pendingEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}"));

            OutboxEvent publishedEvent1 = outboxEventRepository.save(
                new OutboxEvent(AggregateType.CHANNEL, UUID.randomUUID(), "topic2", "{\"data\":\"2\"}"));
            publishedEvent1.markPublished();
            outboxEventRepository.save(publishedEvent1);

            OutboxEvent publishedEvent2 = outboxEventRepository.save(
                new OutboxEvent(AggregateType.MESSAGE, UUID.randomUUID(), "topic3", "{\"data\":\"3\"}"));
            publishedEvent2.markPublished();
            outboxEventRepository.save(publishedEvent2);

            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(
                OutboxEventStatus.PUBLISHED, pageable);

            // then
            assertThat(result).hasSize(2);
            assertThat(result).extracting(OutboxEvent::getStatus)
                .containsOnly(OutboxEventStatus.PUBLISHED);
        }

        @Test
        @DisplayName("FAILED 상태의 이벤트만 조회")
        void findAllByStatusOrderByCreatedAtAsc_failedOnly_returnsFailedEvents() {
            // given
            OutboxEvent pendingEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}"));

            OutboxEvent failedEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.CHANNEL, UUID.randomUUID(), "topic2", "{\"data\":\"2\"}"));
            failedEvent.markFailed();
            outboxEventRepository.save(failedEvent);

            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(
                OutboxEventStatus.FAILED, pageable);

            // then
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getStatus()).isEqualTo(OutboxEventStatus.FAILED);
        }

        @Test
        @DisplayName("createdAt 오름차순으로 정렬되어 반환")
        void findAllByStatusOrderByCreatedAtAsc_orderedByCreatedAtAsc() throws InterruptedException {
            // given
            OutboxEvent event1 = outboxEventRepository.save(
                new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}"));

            Thread.sleep(10); // 생성 시간 차이를 위해 대기

            OutboxEvent event2 = outboxEventRepository.save(
                new OutboxEvent(AggregateType.CHANNEL, UUID.randomUUID(), "topic2", "{\"data\":\"2\"}"));

            Thread.sleep(10);

            OutboxEvent event3 = outboxEventRepository.save(
                new OutboxEvent(AggregateType.MESSAGE, UUID.randomUUID(), "topic3", "{\"data\":\"3\"}"));

            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(
                OutboxEventStatus.PENDING, pageable);

            // then
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getId()).isEqualTo(event1.getId());
            assertThat(result.get(1).getId()).isEqualTo(event2.getId());
            assertThat(result.get(2).getId()).isEqualTo(event3.getId());
        }

        @Test
        @DisplayName("페이지 사이즈만큼만 조회")
        void findAllByStatusOrderByCreatedAtAsc_respectsPageSize() {
            // given
            for (int i = 0; i < 10; i++) {
                outboxEventRepository.save(
                    new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic" + i, "{\"data\":\"" + i + "\"}"));
            }

            Pageable pageable = PageRequest.of(0, 3);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(
                OutboxEventStatus.PENDING, pageable);

            // then
            assertThat(result).hasSize(3);
        }

        @Test
        @DisplayName("해당 상태의 이벤트가 없으면 빈 리스트 반환")
        void findAllByStatusOrderByCreatedAtAsc_noMatchingStatus_returnsEmptyList() {
            // given
            OutboxEvent pendingEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}"));

            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(
                OutboxEventStatus.FAILED, pageable);

            // then
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("이벤트가 전혀 없으면 빈 리스트 반환")
        void findAllByStatusOrderByCreatedAtAsc_noEvents_returnsEmptyList() {
            // given
            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusOrderByCreatedAtAsc(
                OutboxEventStatus.PENDING, pageable);

            // then
            assertThat(result).isEmpty();
        }
    }

    @Nested
    @DisplayName("save")
    class Save {

        @Test
        @DisplayName("OutboxEvent 저장 성공")
        void save_validEvent_persistsEvent() {
            // given
            UUID aggregateId = UUID.randomUUID();
            OutboxEvent event = new OutboxEvent(
                AggregateType.USER,
                aggregateId,
                "discodeit.user.created",
                "{\"userId\":\"" + aggregateId + "\"}"
            );

            // when
            OutboxEvent savedEvent = outboxEventRepository.save(event);

            // then
            assertThat(savedEvent.getId()).isNotNull();
            assertThat(savedEvent.getCreatedAt()).isNotNull();
            assertThat(savedEvent.getAggregateType()).isEqualTo(AggregateType.USER);
            assertThat(savedEvent.getAggregateId()).isEqualTo(aggregateId);
            assertThat(savedEvent.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
        }

        @Test
        @DisplayName("상태 변경 후 저장 시 변경된 상태가 유지됨")
        void save_afterMarkPublished_persistsNewStatus() {
            // given
            OutboxEvent event = outboxEventRepository.save(
                new OutboxEvent(AggregateType.MESSAGE, UUID.randomUUID(), "topic", "{\"data\":\"test\"}"));

            // when
            event.markPublished();
            OutboxEvent updatedEvent = outboxEventRepository.save(event);

            // then
            OutboxEvent foundEvent = outboxEventRepository.findById(updatedEvent.getId()).orElseThrow();
            assertThat(foundEvent.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
            assertThat(foundEvent.getPublishedAt()).isNotNull();
        }
    }

    @Nested
    @DisplayName("findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc")
    class FindAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc {

        @Test
        @DisplayName("지정된 시간 이전에 생성된 FAILED 이벤트만 조회")
        void findFailedEventsBeforeTime_returnsOnlyOldFailedEvents() throws InterruptedException {
            // given
            OutboxEvent oldFailedEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}"));
            oldFailedEvent.markFailed();
            outboxEventRepository.save(oldFailedEvent);

            Thread.sleep(50);
            Instant threshold = Instant.now();
            Thread.sleep(50);

            OutboxEvent newFailedEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.CHANNEL, UUID.randomUUID(), "topic2", "{\"data\":\"2\"}"));
            newFailedEvent.markFailed();
            outboxEventRepository.save(newFailedEvent);

            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                OutboxEventStatus.FAILED, threshold, pageable);

            // then
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getId()).isEqualTo(oldFailedEvent.getId());
        }

        @Test
        @DisplayName("FAILED 상태가 아닌 이벤트는 조회되지 않음")
        void findFailedEventsBeforeTime_excludesNonFailedEvents() {
            // given
            OutboxEvent pendingEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}"));

            OutboxEvent publishedEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.CHANNEL, UUID.randomUUID(), "topic2", "{\"data\":\"2\"}"));
            publishedEvent.markPublished();
            outboxEventRepository.save(publishedEvent);

            OutboxEvent failedEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.MESSAGE, UUID.randomUUID(), "topic3", "{\"data\":\"3\"}"));
            failedEvent.markFailed();
            outboxEventRepository.save(failedEvent);

            Instant futureThreshold = Instant.now().plusSeconds(60);
            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                OutboxEventStatus.FAILED, futureThreshold, pageable);

            // then
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getStatus()).isEqualTo(OutboxEventStatus.FAILED);
        }

        @Test
        @DisplayName("createdAt 오름차순으로 정렬")
        void findFailedEventsBeforeTime_orderedByCreatedAtAsc() throws InterruptedException {
            // given
            OutboxEvent event1 = outboxEventRepository.save(
                new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}"));
            event1.markFailed();
            outboxEventRepository.save(event1);

            Thread.sleep(10);

            OutboxEvent event2 = outboxEventRepository.save(
                new OutboxEvent(AggregateType.CHANNEL, UUID.randomUUID(), "topic2", "{\"data\":\"2\"}"));
            event2.markFailed();
            outboxEventRepository.save(event2);

            Thread.sleep(10);

            OutboxEvent event3 = outboxEventRepository.save(
                new OutboxEvent(AggregateType.MESSAGE, UUID.randomUUID(), "topic3", "{\"data\":\"3\"}"));
            event3.markFailed();
            outboxEventRepository.save(event3);

            Instant futureThreshold = Instant.now().plusSeconds(60);
            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                OutboxEventStatus.FAILED, futureThreshold, pageable);

            // then
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getId()).isEqualTo(event1.getId());
            assertThat(result.get(1).getId()).isEqualTo(event2.getId());
            assertThat(result.get(2).getId()).isEqualTo(event3.getId());
        }

        @Test
        @DisplayName("페이지 사이즈만큼만 조회")
        void findFailedEventsBeforeTime_respectsPageSize() {
            // given
            for (int i = 0; i < 10; i++) {
                OutboxEvent event = outboxEventRepository.save(
                    new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic" + i, "{\"data\":\"" + i + "\"}"));
                event.markFailed();
                outboxEventRepository.save(event);
            }

            Instant futureThreshold = Instant.now().plusSeconds(60);
            Pageable pageable = PageRequest.of(0, 3);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                OutboxEventStatus.FAILED, futureThreshold, pageable);

            // then
            assertThat(result).hasSize(3);
        }

        @Test
        @DisplayName("조건에 맞는 이벤트가 없으면 빈 리스트 반환")
        void findFailedEventsBeforeTime_noMatchingEvents_returnsEmptyList() {
            // given
            OutboxEvent failedEvent = outboxEventRepository.save(
                new OutboxEvent(AggregateType.USER, UUID.randomUUID(), "topic1", "{\"data\":\"1\"}"));
            failedEvent.markFailed();
            outboxEventRepository.save(failedEvent);

            Instant pastThreshold = Instant.now().minusSeconds(3600);
            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                OutboxEventStatus.FAILED, pastThreshold, pageable);

            // then
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("이벤트가 전혀 없으면 빈 리스트 반환")
        void findFailedEventsBeforeTime_noEvents_returnsEmptyList() {
            // given
            Instant threshold = Instant.now();
            Pageable pageable = PageRequest.of(0, 10);

            // when
            List<OutboxEvent> result = outboxEventRepository.findAllByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                OutboxEventStatus.FAILED, threshold, pageable);

            // then
            assertThat(result).isEmpty();
        }
    }
}
