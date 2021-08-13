package com.exactpro.th2.act;

public class FieldNotFoundException extends Exception {
    private static final long serialVersionUID = -6847170993924167906L;

    public FieldNotFoundException() {
    }
    public FieldNotFoundException(String message) {
        super(message);}
    public FieldNotFoundException(String message, Throwable cause) {
        super(message, cause);}
    public FieldNotFoundException(Throwable cause) {
        super(cause);}
    public FieldNotFoundException(String message, Throwable cause,boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);}

}
