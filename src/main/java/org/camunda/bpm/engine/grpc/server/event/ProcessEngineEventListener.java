package org.camunda.bpm.engine.grpc.server.event;

import org.camunda.bpm.spring.boot.starter.event.PostDeployEvent;
import org.camunda.bpm.spring.boot.starter.event.PreUndeployEvent;
import org.springframework.context.event.EventListener;

public interface ProcessEngineEventListener {

    @EventListener
    void onPostDeploy(PostDeployEvent event);

    @EventListener
    void onPreUndeploy(PreUndeployEvent event);
}
