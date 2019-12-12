# go-circuitbreaker

[![GoDoc](https://godoc.org/github.com/mercari/go-circuitbreaker?status.svg)](https://godoc.org/github.com/mercari/go-circuitbreaker)
[![CircleCI](https://circleci.com/gh/mercari/go-circuitbreaker.svg?style=svg)](https://circleci.com/gh/mercari/go-circuitbreaker)

go-circuitbreaker is a Circuit Breaker pattern implementation in Go.

- Provides natural code flow.
- Ignore errors occurred by request cancellation from request callers (in default).
- `Ignore(err)` and `MarkAsSuccess(err)` wrappers enable you to receive non-nil error from wrapped operations without counting it as a failure.

# What is circuit breaker?

See: [Circuit Breaker pattern \- Cloud Design Patterns \| Microsoft Docs](https://docs.microsoft.com/en-us/azure/architecture/patterns/circuit-breaker)

## Initializing the circuit breaker

This library is using the [functional options pattern](https://github.com/uber-go/guide/blob/master/style.md#functional-options) to instance a new circuit breaker. The functions are the following:

```go
// Set the function for counter
WithShouldTrip(tripFunc TripFunc)

// Set the clock
WithClock(clock clock.Clock)

// Set the time backoff
WithBackOff(backoff backoff.BackOff) 

// Set the timeout of the circuit breaker
WithTimeout(timeout time.Duration)

// Set the number of half open successes
WithHalfOpenMaxSuccesses(maxSuccesses int64)

// Set the interval of the circuit breaker
WithInterval(interval time.Duration)

// Set if the context should fail on cancel
WithFailOnContextCancel(failOnContextCancel bool) 

// Set if the context should fail on deadline
WithFailOnContextDeadline(failOnContextDeadline bool)
```

You can use them like the next example:

```go
var cb = circuitbreaker.New(circuitbreaker.WithInterval(1000*time.Milliseconds), 
                            circuitbreaker.WithHalfOpenMaxSuccesses(4))
```

## Simple Examples with Do.

Wrapping your code block with `Do()` protects your process with Circuit Breaker.

```go
var cb = circuitbreaker.New()

u, err := cb.Do(ctx, func() (interface{}, error) {
  return fetchUserInfo(name)
})
user, _ := u.(*User) // Casting interface{} into *User safely.
```

## Example using Done.

The following example using Ready() and Done() is exactly equals to the above one. Since this style enables you to protect your processes without wrapping it and using type-unsafe interface{}, it would make it easy to implement CB to your existing logic.

```go
var cb = circuitbreaker.New()

func getUserInfo(ctx context.Context, name string) (_ *User,err error) {
  if !cb.Ready() {
    return nil, circuitbreaker.ErrOpened
  }
  defer func() { err = cb.Done(ctx, err) }

  return fetchUserInfo(ctx, name)
}
```


## Smart handling for context.Canceled and context.DeadlineExceeded

In microservices architectures, Circuit Breaker is essential to protect services you depend on from cascading failures and keep your services latency low during long-lasting failures. And for microservices written in Go, cancel request by propagating context is also a good convention. But there are some pitfall when you combinate context cnad circuit breaker.

Your microservices' users are able to cancel your requests. The cancellation will be propagated through the context to your operation which is protected by CB. If the CB mark the canceled execution as a fail, your CB may open even though there is no problem in your infrastructure. It is a false-positive CB-open. It would prevent your other clients from doing tasks successfully because the CB is shared by all clients. Otherwise, if the CB mark the canceled execution as a success, the CB may back to closed unexpectedly. (it's a false-negative). In order to avoid those unexpected issue, `go-circuitbreaker` provides *an option to ignore the errors caused by the context cancellation*.

The same for timeouts configuration. Especially on gRPC, clients are able to set a timeout to the server's context. It means that clients with too short timeout are able to make other clients operation fail. For this issue, `go-circuitbreaker` provides an option to *an option to ignore deadline excceded errors* .

## Ignore and MarkAsSuccess wrappers

Sometimes, we would like to receive an error from a protected operation but don't want to mark the request as a failure. For example, a case that protected HTTP request responded 404 NotFound. This application-level error is obviously not caused by a failure, so that we'd like to return nil error, but want to receive non-nil error. Because `go-circuitbreaker` is able to receive errors from protected operations without making them as failures, you don't need to write complicated error handlings in order to achieve your goal.

```go
cb := circuitbreaker.New(nil)

data, err := cb.Do(context.Background(), func() (interface{}, error) {
  u, err := fetchUserInfo("john")
  if err == errUserNotFound {
    return circuitbreaker.Ignore(err) // cb does not treat the err as a failure.
  }
  return u, err
})
if err != nil {
  // Here, you can get errUserNotFound
  log.Fatal(err)
}
```

## Installation

```bash
go get github.com/mercari/go-circuitbreaker
```
