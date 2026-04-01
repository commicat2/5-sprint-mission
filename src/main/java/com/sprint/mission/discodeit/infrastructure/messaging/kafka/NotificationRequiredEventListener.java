package com.sprint.mission.discodeit.infrastructure.messaging.kafka;

import com.sprint.mission.discodeit.auth.domain.event.RoleUpdatedEvent;
import com.sprint.mission.discodeit.binarycontent.domain.event.BinaryContentStorageFailedEvent;
import com.sprint.mission.discodeit.message.domain.Message;
import com.sprint.mission.discodeit.message.domain.MessageRepository;
import com.sprint.mission.discodeit.message.domain.event.MessageCreatedEvent;
import com.sprint.mission.discodeit.notification.application.NotificationService;
import com.sprint.mission.discodeit.readstatus.domain.ReadStatus;
import com.sprint.mission.discodeit.readstatus.domain.ReadStatusRepository;
import com.sprint.mission.discodeit.user.domain.Role;
import com.sprint.mission.discodeit.user.domain.User;
import com.sprint.mission.discodeit.user.domain.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationRequiredEventListener {

    private final NotificationService notificationService;
    private final MessageRepository messageRepository;
    private final ReadStatusRepository readStatusRepository;
    private final UserRepository userRepository;

    @KafkaListener(topics = MessageCreatedEvent.TOPIC, groupId = "notification-group")
    public void onMessageCreated(MessageCreatedEvent event) {
        Message message = messageRepository.findWithAuthorAndChannelById(event.messageId())
            .orElse(null);
        if (message == null) {
            log.warn("알림 대상 메시지를 찾을 수 없음: messageId={}", event.messageId());
            return;
        }

        List<ReadStatus> targets = readStatusRepository.findNotificationTargets(
            message.getChannel().getId(), message.getAuthor().getId());

        String title = "%s (#%s)".formatted(
            message.getAuthor().getUsername(),
            message.getChannel().getName() != null ? message.getChannel().getName() : "DM"
        );

        targets.forEach(target ->
            notificationService.create(target.getUser().getId(), title, message.getContent())
        );
    }

    @KafkaListener(topics = RoleUpdatedEvent.TOPIC, groupId = "notification-group")
    public void onRoleUpdated(RoleUpdatedEvent event) {
        String title = "권한이 변경되었습니다.";
        String content = "%s -> %s".formatted(event.oldRole(), event.newRole());

        notificationService.create(event.userId(), title, content);
    }

    @KafkaListener(topics = BinaryContentStorageFailedEvent.TOPIC, groupId = "notification-group")
    public void onBinaryContentStorageFailed(BinaryContentStorageFailedEvent event) {
        String title = "파일 업로드 실패";
        String content = "Task: BinaryContentStorage%nRequestId: %s%nBinaryContentId: %s%nError: %s"
            .formatted(
                event.requestId() != null ? event.requestId() : "N/A",
                event.binaryContentId(),
                event.errorMessage()
            );

        List<User> admins = userRepository.findAllByRole(Role.ADMIN);
        admins.forEach(admin -> notificationService.create(admin.getId(), title, content));
    }
}
