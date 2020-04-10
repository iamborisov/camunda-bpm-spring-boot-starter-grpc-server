package org.camunda.bpm.engine.grpc.server.event.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.grpc.server.consumer.ExternalTaskCreationConsumer;
import org.camunda.bpm.engine.grpc.server.event.ProcessEngineEventListener;
import org.camunda.bpm.spring.boot.starter.event.PostDeployEvent;
import org.camunda.bpm.spring.boot.starter.event.PreUndeployEvent;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessEngineEventListenerimpl implements ProcessEngineEventListener {

    private final ExternalTaskCreationConsumer externalTaskCreationConsumer;

    @Override
    public void onPostDeploy(PostDeployEvent event) {
        log.info("Triggered PostDeployEvent event");

        try {
            externalTaskCreationConsumer.start();
        } catch (ExternalTaskCreationConsumer.AlreadyStartedException e) {
            log.error("Consumer already started", e);
        }
    }

    @Override
    public void onPreUndeploy(PreUndeployEvent event) {
        log.info("Triggered PreUndeployEvent event");

        try {
            externalTaskCreationConsumer.stop();
        } catch (ExternalTaskCreationConsumer.AlreadyStoppedException e) {
            log.error("Consumer already stopped", e);
        }
    }
}