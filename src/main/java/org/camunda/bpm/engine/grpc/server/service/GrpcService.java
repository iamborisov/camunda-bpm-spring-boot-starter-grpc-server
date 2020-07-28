package org.camunda.bpm.engine.grpc.server.service;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.grpc.CompleteRequest;
import org.camunda.bpm.engine.grpc.CompleteResponse;
import org.camunda.bpm.engine.grpc.CreateMessageRequest;
import org.camunda.bpm.engine.grpc.CreateMessageResponse;
import org.camunda.bpm.engine.grpc.DeleteProcessInstanceRequest;
import org.camunda.bpm.engine.grpc.DeleteProcessInstanceResponse;
import org.camunda.bpm.engine.grpc.ExtendLockRequest;
import org.camunda.bpm.engine.grpc.ExtendLockResponse;
import org.camunda.bpm.engine.grpc.ExternalTaskGrpc.ExternalTaskImplBase;
import org.camunda.bpm.engine.grpc.FetchAndLockRequest;
import org.camunda.bpm.engine.grpc.FetchAndLockResponse;
import org.camunda.bpm.engine.grpc.HandleBpmnErrorRequest;
import org.camunda.bpm.engine.grpc.HandleBpmnErrorResponse;
import org.camunda.bpm.engine.grpc.HandleFailureRequest;
import org.camunda.bpm.engine.grpc.HandleFailureResponse;
import org.camunda.bpm.engine.grpc.UnlockRequest;
import org.camunda.bpm.engine.grpc.UnlockResponse;
import org.camunda.bpm.engine.grpc.server.repository.ConnectionRepository;
import org.camunda.bpm.engine.impl.MessageCorrelationBuilderImpl;
import org.camunda.bpm.engine.impl.interceptor.CommandExecutor;
import org.camunda.bpm.engine.runtime.MessageCorrelationBuilder;
import org.camunda.spin.plugin.variable.value.impl.JsonValueImpl;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class GrpcService extends ExternalTaskImplBase {

    private final ExternalTaskService externalTaskService;

    private final RuntimeService runtimeService;

    private final ConnectionRepository connectionRepository;

    private final CommandExecutor commandExecutor;

    @Override
    public StreamObserver<FetchAndLockRequest> fetchAndLock(StreamObserver<FetchAndLockResponse> responseObserver) {
        return new StreamObserver<>() {

            @Override
            public void onNext(FetchAndLockRequest request) {
                log.info("fetchAndLock::onNext", request);

                connectionRepository.remove(responseObserver);
                connectionRepository.add(request, responseObserver);
            }

            @Override
            public void onError(Throwable t) {
                log.error("fetchAndLock::onError", t);

                connectionRepository.remove(responseObserver);
            }

            @Override
            public void onCompleted() {
                log.info("fetchAndLock::onCompleted");

                connectionRepository.remove(responseObserver);
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void complete(CompleteRequest request, StreamObserver<CompleteResponse> responseObserver) {
        log.info("Complete external task", request);

        try {
            externalTaskService.complete(
                request.getId(),
                request.getWorkerId(),
                assembleVariables(request.getVariables())
            );

            responseObserver.onNext(CompleteResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on completing task " + request.getId(), e);
            responseObserver.onNext(CompleteResponse.newBuilder().setStatus("500").build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void handleFailure(HandleFailureRequest request, StreamObserver<HandleFailureResponse> responseObserver) {
        log.info("Handle external task failure", request);

        try {
            externalTaskService.handleFailure(
                request.getId(),
                request.getWorkerId(),
                request.getErrorMessage(),
                request.getErrorDetails(),
                request.getRetries(),
                request.getRetryTimeout()
            );

            responseObserver.onNext(HandleFailureResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on handling failure for task " + request.getId(), e);
            responseObserver.onNext(HandleFailureResponse.newBuilder().setStatus("500").build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void handleBpmnError(HandleBpmnErrorRequest request, StreamObserver<HandleBpmnErrorResponse> responseObserver) {
        log.info("Handle external task BPMN error", request);

        try {
            externalTaskService.handleBpmnError(
                request.getId(),
                request.getWorkerId(),
                request.getErrorCode(),
                request.getErrorMessage(),
                assembleVariables(request.getVariables())
            );

            responseObserver.onNext(HandleBpmnErrorResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on handling BPMN error for task " + request.getId(), e);
            responseObserver.onNext(HandleBpmnErrorResponse.newBuilder().setStatus("500").build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void unlock(UnlockRequest request, StreamObserver<UnlockResponse> responseObserver) {
        log.info("Unlock external task", request);

        try {
            externalTaskService.unlock(request.getId());
            responseObserver.onNext(UnlockResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on unlocking task " + request.getId(), e);
            responseObserver.onNext(UnlockResponse.newBuilder().setStatus("500").build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void extendLock(ExtendLockRequest request, StreamObserver<ExtendLockResponse> responseObserver) {
        log.info("Lock external task", request);

        try {
            externalTaskService.extendLock(request.getId(), request.getWorkerId(), request.getDuration());
            responseObserver.onNext(ExtendLockResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on extending lock for task " + request.getId(), e);
            responseObserver.onNext(ExtendLockResponse.newBuilder().setStatus("500").build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void createMessage(CreateMessageRequest request, StreamObserver<CreateMessageResponse> responseObserver) {
        try {
            log.info(String.format("Correlating message %s", request.getMessageName()));

            MessageCorrelationBuilder messageCorrelationBuilder = new MessageCorrelationBuilderImpl(commandExecutor, request.getMessageName());

            if (!request.getProcessInstanceId().isEmpty()) {
                messageCorrelationBuilder.processInstanceId(request.getProcessInstanceId());
            }

            if (!request.getVariables().isEmpty()) {
                messageCorrelationBuilder.setVariables(assembleVariables(request.getVariables()));
            }

            if (!request.getBusinessKey().isEmpty()) {
                messageCorrelationBuilder.processInstanceBusinessKey(request.getBusinessKey());
            }

            messageCorrelationBuilder.correlate();

            responseObserver.onNext(CreateMessageResponse.newBuilder().setProcessInstanceId(request.getProcessInstanceId() == null ? "" : request.getProcessInstanceId()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on correlating message " + request.getMessageName(), e);
            responseObserver.onNext(CreateMessageResponse.newBuilder().setProcessInstanceId(request.getProcessInstanceId() == null ? "" : request.getProcessInstanceId()).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void deleteProcessInstance(DeleteProcessInstanceRequest request, StreamObserver<DeleteProcessInstanceResponse> responseObserver) {
        log.info("Deleting process instance '{}'", request);

        try {
            runtimeService.deleteProcessInstance(request.getProcessInstanceId(), request.getReason());
            responseObserver.onNext(DeleteProcessInstanceResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on deleting process instance '{}'", request.getProcessInstanceId(), e);
            responseObserver.onNext(DeleteProcessInstanceResponse.newBuilder().setStatus("500").build());
            responseObserver.onCompleted();
        }
    }

    private Map<String, Object> assembleVariables(String jsonValue) {
        return Collections.singletonMap("variables", new JsonValueImpl(jsonValue).getValue());
    }
}
