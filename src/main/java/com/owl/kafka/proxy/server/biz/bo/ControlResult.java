package com.owl.kafka.proxy.server.biz.bo;

/**
 * @Author: Tboy
 */
public class ControlResult {

    public static final ControlResult ALLOWED = new ControlResult(true);

    private boolean allowed;

    private String message;

    public ControlResult(boolean allowed){
        this.allowed = allowed;
        this.message = null;
    }

    public ControlResult(boolean allowed, String message){
        this.allowed = allowed;
        this.message = message;
    }

    public boolean isAllowed() {
        return allowed;
    }

    public void setAllowed(boolean allowed) {
        this.allowed = allowed;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
