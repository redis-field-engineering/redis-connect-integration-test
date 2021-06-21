package com.redislabs.connect.integration.test.source.rdb;

import com.redislabs.connect.ConnectConstants;
import com.redislabs.connect.core.model.ChangeEvent;
import com.redislabs.connect.model.Operation;
import com.redislabs.connect.transformer.Transformer;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 *
 * @author Virag Tripathi
 *
 */

@Slf4j
public class IntegrityTestPostProcessor implements Transformer<ChangeEvent<Map<String, Object>>, Object> {

    @Override
    public String getId() {
        return "IntegrityTestPostProcessor";
    }

    /**
     * Performs this operation on the given arguments.
     *
     * @param changeEvent the first input argument
     * @param o              the second input argument
     */
    @Override
    public void accept(ChangeEvent<Map<String, Object>> changeEvent, Object o) {
        log.info("In IntegrityTestPostProcessor : {}", changeEvent);

        if (changeEvent.getPayload() != null && changeEvent.getPayload().get(ConnectConstants.VALUE) != null) {
            Operation op = (Operation) changeEvent.getPayload().get(ConnectConstants.VALUE);
            log.info("IntegrityTestPostProcessor::accept : {}, table : {}, operation : {}", getId(),
                    op.getTable(), op.getType());

        }

    }
}
