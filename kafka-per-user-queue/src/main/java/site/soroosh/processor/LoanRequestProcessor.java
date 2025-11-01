package site.soroosh.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import site.soroosh.AppProps;
import site.soroosh.converter.PendingRequestConverter;
import site.soroosh.schema.InflightState;
import site.soroosh.schema.LoanRequest;

import java.util.Collections;
import java.util.List;

public class LoanRequestProcessor implements Processor<String, LoanRequest, String, LoanRequest> {
    private KeyValueStore<String, InflightState> inflightStateStore;
    private KeyValueStore<String, LoanRequest> pendingRequestsStore;
    private ProcessorContext<String, LoanRequest> ctx;

    @Override
    public void init(ProcessorContext<String, LoanRequest> context) {
        ctx = context;
        inflightStateStore = context.getStateStore(AppProps.INFLIGHT_STATE_STORE_NAME);
        pendingRequestsStore = context.getStateStore(AppProps.PENDING_REQUESTS_STORE_NAME);
    }

    @Override
    public void process(Record<String, LoanRequest> incomingRecord) {
        String userId = incomingRecord.key();
        LoanRequest incomingRequest = incomingRecord.value();
        String incomingRequestId = incomingRequest.getRequestId().toString();

        InflightState currentInflightState = inflightStateStore.get(userId);

        if (currentInflightState == null) {
            // No inflight message
            InflightState newState = new InflightState(
                    incomingRequestId,
                    Collections.emptyList()
            );

            inflightStateStore.put(userId, newState);
            ctx.forward(incomingRecord);

            return;
        }

        // There's a message already inflight

        List<CharSequence> currentPendingRequestIds = currentInflightState.getPendingRequestIds();
        currentPendingRequestIds.add(incomingRequestId); // Update in place

        inflightStateStore.put(userId, currentInflightState);
        pendingRequestsStore.put(PendingRequestConverter.getPendingRequestMessageKey(userId, incomingRequestId), incomingRequest);
    }
}