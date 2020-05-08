package org.camunda.bpm.engine.grpc.server.informer;

import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.externaltask.ExternalTaskQueryBuilder;
import org.camunda.bpm.engine.externaltask.ExternalTaskQueryTopicBuilder;
import org.camunda.bpm.engine.grpc.FetchAndLockRequest;

import java.util.Collection;

import static java.lang.Boolean.TRUE;

public interface ExternalTaskQuery {

    static ExternalTaskQueryBuilder create(FetchAndLockRequest request, ExternalTaskService externalTaskService) {
        ExternalTaskQueryBuilder fetchBuilder = externalTaskService.fetchAndLock(
            request.getMaxTasks(),
            request.getWorkerId(),
            request.getUsePriority());

        if (request.getTopicList() != null) {
            for (FetchAndLockRequest.FetchExternalTaskTopic topicDto : request.getTopicList()) {
                ExternalTaskQueryTopicBuilder topicFetchBuilder = fetchBuilder.topic(topicDto.getTopicName(), topicDto.getLockDuration());

                if (notEmpty(topicDto.getBusinessKey())) {
                    topicFetchBuilder = topicFetchBuilder.businessKey(topicDto.getBusinessKey());
                }

                if (notEmpty(topicDto.getProcessDefinitionId())) {
                    topicFetchBuilder.processDefinitionId(topicDto.getProcessDefinitionId());
                }

                if (notEmpty(topicDto.getProcessDefinitionIdInList())) {
                    topicFetchBuilder.processDefinitionIdIn(topicDto.getProcessDefinitionIdInList().toArray(new String[topicDto.getProcessDefinitionIdInList().size()]));
                }

                if (notEmpty(topicDto.getProcessDefinitionKey())) {
                    topicFetchBuilder.processDefinitionKey(topicDto.getProcessDefinitionKey());
                }

                if (notEmpty(topicDto.getProcessDefinitionKeyInList())) {
                    topicFetchBuilder.processDefinitionKeyIn(topicDto.getProcessDefinitionKeyInList().toArray(new String[topicDto.getProcessDefinitionKeyInList().size()]));
                }

                if (topicDto.getDeserializeValues()) {
                    topicFetchBuilder = topicFetchBuilder.enableCustomObjectDeserialization();
                }

                if (topicDto.getLocalVariables()) {
                    topicFetchBuilder = topicFetchBuilder.localVariables();
                }

                if (TRUE.equals(topicDto.getWithoutTenantId())) {
                    topicFetchBuilder = topicFetchBuilder.withoutTenantId();
                }

                if (notEmpty(topicDto.getTenantIdInList())) {
                    topicFetchBuilder = topicFetchBuilder.tenantIdIn(topicDto.getTenantIdInList().toArray(new String[topicDto.getTenantIdInList().size()]));
                }

                if (notEmpty(topicDto.getProcessDefinitionVersionTag())) {
                    topicFetchBuilder = topicFetchBuilder.processDefinitionVersionTag(topicDto.getProcessDefinitionVersionTag());
                }

                fetchBuilder = topicFetchBuilder;
            }
        }

        return fetchBuilder;
    }

    private static boolean notEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    private static boolean notEmpty(Collection<?> list) {
        return list != null && !list.isEmpty();
    }
}
