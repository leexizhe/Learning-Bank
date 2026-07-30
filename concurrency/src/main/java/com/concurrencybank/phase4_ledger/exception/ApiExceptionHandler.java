package com.concurrencybank.phase4_ledger.exception;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(AccountNotFoundException.class)
    public ProblemDetail handleNotFound(AccountNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(InsufficientFundsException.class)
    public ProblemDetail handleInsufficientFunds(InsufficientFundsException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
    }

    /**
     * {@code Account.version} is what makes this reachable at all: every write
     * still goes through {@code TransferService}'s pessimistic row lock, but if
     * anything ever updated a row outside that path, this is the safety net
     * that turns a silent lost update into a loud, retryable 409.
     */
    @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
    public ProblemDetail handleOptimisticLockFailure(ObjectOptimisticLockingFailureException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "Account was modified concurrently, retry the request");
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ProblemDetail handleConflict(DataIntegrityViolationException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "Conflicting write: " + e.getMostSpecificCause().getMessage());
    }
}
