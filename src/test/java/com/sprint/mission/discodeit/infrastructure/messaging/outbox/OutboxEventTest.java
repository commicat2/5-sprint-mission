package com.sprint.mission.discodeit.infrastructure.messaging.outbox;

import com.sprint.mission.discodeit.common.infrastructure.outbox.AggregateType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("OutboxEvent 도메인 테스트")
class OutboxEventTest {

    @Nested
    @SuppressWarnings("ConstantConditions")
    @DisplayName("생성자")
    class Constructor {

        @Test
        @DisplayName("유효한 인자로 OutboxEvent 생성 성공")
        void constructor_validArguments_createsOutboxEvent() {
            // given
            AggregateType aggregateType = AggregateType.USER;
            UUID aggregateId = UUID.randomUUID();
            String topic = "test.topic";
            String payload = "{\"data\":\"test\"}";

            // when
            OutboxEvent event = new OutboxEvent(aggregateType, aggregateId, topic, payload);

            // then
            assertThat(event.getAggregateType()).isEqualTo(aggregateType);
            assertThat(event.getAggregateId()).isEqualTo(aggregateId);
            assertThat(event.getTopic()).isEqualTo(topic);
            assertThat(event.getPayload()).isEqualTo(payload);
        }

        @ParameterizedTest
        @EnumSource(AggregateType.class)
        @DisplayName("모든 AggregateType으로 OutboxEvent 생성 가능")
        void constructor_allAggregateTypes_createsOutboxEvent(AggregateType aggregateType) {
            // given
            UUID aggregateId = UUID.randomUUID();
            String topic = "test.topic";
            String payload = "{\"data\":\"test\"}";

            // when
            OutboxEvent event = new OutboxEvent(aggregateType, aggregateId, topic, payload);

            // then
            assertThat(event.getAggregateType()).isEqualTo(aggregateType);
        }

        @Test
        @DisplayName("aggregateType이 null이면 IllegalArgumentException 발생")
        void constructor_nullAggregateType_throwsException() {
            // given
            UUID aggregateId = UUID.randomUUID();
            String topic = "test.topic";
            String payload = "{\"data\":\"test\"}";

            // when & then
            assertThatThrownBy(() -> new OutboxEvent(null, aggregateId, topic, payload))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("aggregateId가 null이면 IllegalArgumentException 발생")
        void constructor_nullAggregateId_throwsException() {
            // given
            AggregateType aggregateType = AggregateType.USER;
            String topic = "test.topic";
            String payload = "{\"data\":\"test\"}";

            // when & then
            assertThatThrownBy(() -> new OutboxEvent(aggregateType, null, topic, payload))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("topic이 null이면 IllegalArgumentException 발생")
        void constructor_nullTopic_throwsException() {
            // given
            AggregateType aggregateType = AggregateType.USER;
            UUID aggregateId = UUID.randomUUID();
            String payload = "{\"data\":\"test\"}";

            // when & then
            assertThatThrownBy(() -> new OutboxEvent(aggregateType, aggregateId, null, payload))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("payload가 null이면 IllegalArgumentException 발생")
        void constructor_nullPayload_throwsException() {
            // given
            AggregateType aggregateType = AggregateType.USER;
            UUID aggregateId = UUID.randomUUID();
            String topic = "test.topic";

            // when & then
            assertThatThrownBy(() -> new OutboxEvent(aggregateType, aggregateId, topic, null))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("payload가 빈 문자열이면 IllegalArgumentException 발생")
        void constructor_emptyPayload_throwsException() {
            // given
            AggregateType aggregateType = AggregateType.USER;
            UUID aggregateId = UUID.randomUUID();
            String topic = "test.topic";

            // when & then
            assertThatThrownBy(() -> new OutboxEvent(aggregateType, aggregateId, topic, ""))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("payload가 공백만 있으면 IllegalArgumentException 발생")
        void constructor_blankPayload_throwsException() {
            // given
            AggregateType aggregateType = AggregateType.USER;
            UUID aggregateId = UUID.randomUUID();
            String topic = "test.topic";

            // when & then
            assertThatThrownBy(() -> new OutboxEvent(aggregateType, aggregateId, topic, "   "))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("복잡한 JSON payload로 OutboxEvent 생성 성공")
        void constructor_complexJsonPayload_createsOutboxEvent() {
            // given
            AggregateType aggregateType = AggregateType.MESSAGE;
            UUID aggregateId = UUID.randomUUID();
            String topic = "discodeit.message.created";
            String payload = """
                {
                    "messageId": "123e4567-e89b-12d3-a456-426614174000",
                    "content": "Hello, World!",
                    "author": {
                        "id": "user-123",
                        "name": "testuser"
                    },
                    "attachments": [
                        {"id": "file-1", "name": "image.png"},
                        {"id": "file-2", "name": "document.pdf"}
                    ]
                }
                """;

            // when
            OutboxEvent event = new OutboxEvent(aggregateType, aggregateId, topic, payload);

            // then
            assertThat(event.getPayload()).isEqualTo(payload);
        }

        @Test
        @DisplayName("생성 시 기본 상태는 PENDING")
        void constructor_defaultStatus_isPending() {
            // given
            AggregateType aggregateType = AggregateType.USER;
            UUID aggregateId = UUID.randomUUID();
            String topic = "test.topic";
            String payload = "{\"data\":\"test\"}";

            // when
            OutboxEvent event = new OutboxEvent(aggregateType, aggregateId, topic, payload);

            // then
            assertThat(event.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
            assertThat(event.getPublishedAt()).isNull();
        }
    }

    @Nested
    @DisplayName("markPublished")
    class MarkPublished {

        @Test
        @DisplayName("발행 완료 시 상태가 PUBLISHED로 변경되고 publishedAt이 설정됨")
        void markPublished_changesStatusAndSetsPublishedAt() {
            // given
            OutboxEvent event = new OutboxEvent(
                AggregateType.USER,
                UUID.randomUUID(),
                "test.topic",
                "{\"data\":\"test\"}"
            );

            // when
            event.markPublished();

            // then
            assertThat(event.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
            assertThat(event.getPublishedAt()).isNotNull();
        }

        @Test
        @DisplayName("PENDING 상태에서 PUBLISHED로 변경 가능")
        void markPublished_fromPending_succeeds() {
            // given
            OutboxEvent event = new OutboxEvent(
                AggregateType.MESSAGE,
                UUID.randomUUID(),
                "discodeit.message.created",
                "{\"messageId\":\"123\"}"
            );
            assertThat(event.getStatus()).isEqualTo(OutboxEventStatus.PENDING);

            // when
            event.markPublished();

            // then
            assertThat(event.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
        }
    }

    @Nested
    @DisplayName("markFailed")
    class MarkFailed {

        @Test
        @DisplayName("발행 실패 시 상태가 FAILED로 변경됨")
        void markFailed_changesStatusToFailed() {
            // given
            OutboxEvent event = new OutboxEvent(
                AggregateType.USER,
                UUID.randomUUID(),
                "test.topic",
                "{\"data\":\"test\"}"
            );

            // when
            event.markFailed();

            // then
            assertThat(event.getStatus()).isEqualTo(OutboxEventStatus.FAILED);
            assertThat(event.getPublishedAt()).isNull();
        }

        @Test
        @DisplayName("PENDING 상태에서 FAILED로 변경 가능")
        void markFailed_fromPending_succeeds() {
            // given
            OutboxEvent event = new OutboxEvent(
                AggregateType.CHANNEL,
                UUID.randomUUID(),
                "discodeit.channel.deleted",
                "{\"channelId\":\"456\"}"
            );
            assertThat(event.getStatus()).isEqualTo(OutboxEventStatus.PENDING);

            // when
            event.markFailed();

            // then
            assertThat(event.getStatus()).isEqualTo(OutboxEventStatus.FAILED);
        }
    }
}
