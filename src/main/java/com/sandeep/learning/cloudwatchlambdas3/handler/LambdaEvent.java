package com.sandeep.learning.cloudwatchlambdas3.handler;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class LambdaEvent {

    private String eventSource;

    private String functionName;

    private String lambdaType;

    private String arn;

    private String functionVersion;

    private String requestId;

    private String eventTime;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (getLambdaType() != null)
            sb.append("\"eventSource\" : ").append("\"").append(getEventSource()).append("\"").append(",");
        if (getLambdaType() != null)
            sb.append("\"lambdaType\" : ").append("\"").append(getLambdaType()).append("\"").append(",");
        if (getFunctionName() != null)
            sb.append("\"functionName\" : ").append("\"").append(getFunctionName()).append("\"").append(",");
        if (getFunctionVersion() != null)
            sb.append("\"functionVersion\" : ").append("\"").append(getFunctionVersion()).append("\"").append(",");
        if (getArn() != null)
            sb.append("\"arn\" : ").append("\"").append(getArn()).append("\"").append(",");
        if (getRequestId() != null)
            sb.append("\"requestId\" : ").append("\"").append(getRequestId()).append("\"").append(",");
        if (getEventTime() != null)
            sb.append("\"eventTime\" : ").append("\"").append(getEventTime()).append("\"");
        sb.append("}");
        return sb.toString();
    }

}
