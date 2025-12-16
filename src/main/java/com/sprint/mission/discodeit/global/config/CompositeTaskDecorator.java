package com.sprint.mission.discodeit.global.config;

import org.springframework.core.task.TaskDecorator;
import org.springframework.lang.NonNull;

import java.util.List;

public class CompositeTaskDecorator implements TaskDecorator {

    private final List<TaskDecorator> decorators;

    public CompositeTaskDecorator(TaskDecorator... decorators) {
        this.decorators = List.of(decorators);
    }

    @Override
    @NonNull
    public Runnable decorate(@NonNull Runnable runnable) {
        Runnable decorated = runnable;
        for (TaskDecorator decorator : decorators) {
            decorated = decorator.decorate(decorated);
        }
        return decorated;
    }
}
