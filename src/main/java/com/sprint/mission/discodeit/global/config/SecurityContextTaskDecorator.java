package com.sprint.mission.discodeit.global.config;

import org.springframework.core.task.TaskDecorator;
import org.springframework.lang.NonNull;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

public class SecurityContextTaskDecorator implements TaskDecorator {

    @Override
    @NonNull
    public Runnable decorate(@NonNull Runnable runnable) {
        SecurityContext securityContext = SecurityContextHolder.getContext();

        return () -> {
            try {
                SecurityContextHolder.setContext(securityContext);
                runnable.run();
            } finally {
                SecurityContextHolder.clearContext();
            }
        };
    }
}
