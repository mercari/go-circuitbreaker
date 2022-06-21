package circuitbreaker

import (
	"github.com/benbjohnson/clock"
	"github.com/cenkalti/backoff/v3"
)

// each implementations of state represents State of circuit breaker.
//
// ref: https://docs.microsoft.com/en-us/azure/architecture/patterns/circuit-breaker
type state[T any] interface {
	State() State[T]
	onEntry(cb *CircuitBreaker[T])
	onExit(cb *CircuitBreaker[T])
	ready(cb *CircuitBreaker[T]) bool
	onSuccess(cb *CircuitBreaker[T])
	onFail(cb *CircuitBreaker[T])
}

// [Closed state]
//   /onEntry
//      - Reset counters.
//      - Start ticker.
//   /ready
//      - returns true.
//   /onFail
//      - update counters.
//      - If threshold reached, change state to [Open]
//   /onTicker
//      - reset counters.
//   /onExit
//      - stop ticker.
type stateClosed[T any] struct {
	ticker *clock.Ticker
	done   chan struct{}
}

func (st *stateClosed[T]) State() State[T] { return State[T](StateClosed) }
func (st *stateClosed[T]) onEntry(cb *CircuitBreaker[T]) {
	cb.cnt.resetFailures()
	cb.openBackOff.Reset()
	if cb.interval > 0 {
		st.ticker = cb.clock.Ticker(cb.interval)
		st.done = make(chan struct{})
		go func() {
			for {
				select {
				case <-st.ticker.C:
					st.onTicker(cb)
				case <-st.done:
					st.ticker.Stop()
					return
				}
			}
		}()
	}
}

func (st *stateClosed[T]) onExit(cb *CircuitBreaker[T]) {
	if st.done != nil {
		close(st.done)
	}
}

func (st *stateClosed[T]) onTicker(cb *CircuitBreaker[T]) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.cnt.reset()
}

func (st *stateClosed[T]) ready(cb *CircuitBreaker[T]) bool { return true }
func (st *stateClosed[T]) onSuccess(cb *CircuitBreaker[T])  {}
func (st *stateClosed[T]) onFail(cb *CircuitBreaker[T]) {
	if cb.shouldTrip(&cb.cnt) {
		cb.setState(&stateOpen[T]{})
	}
}

// [Open state]
//   /onEntry
//      - Start timer.
//   /ready
//      - Returns false.
//   /onTimer
//     - Change state to [HalfOpen].
//   /onExit
//     - Stop timer.
type stateOpen[T any] struct {
	timer *clock.Timer
}

func (st *stateOpen[T]) State() State[T] { return State[T](StateOpen) }
func (st *stateOpen[T]) onEntry(cb *CircuitBreaker[T]) {
	timeout := cb.openBackOff.NextBackOff()
	if timeout != backoff.Stop {
		st.timer = cb.clock.AfterFunc(timeout, func() { st.onTimer(cb) })
	}
}

func (st *stateOpen[T]) onTimer(cb *CircuitBreaker[T])    { cb.setStateWithLock(&stateHalfOpen[T]{}) }
func (st *stateOpen[T]) onExit(cb *CircuitBreaker[T])     { st.timer.Stop() }
func (st *stateOpen[T]) ready(cb *CircuitBreaker[T]) bool { return false }
func (st *stateOpen[T]) onSuccess(cb *CircuitBreaker[T])  {}
func (st *stateOpen[T]) onFail(cb *CircuitBreaker[T])     {}

// [HalfOpen state]
//   /ready
//      -> returns true
//   /onSuccess
//      -> Increment Success counter.
//      -> If threshold reached, change state to [Closed].
//   /onFail
//      -> change state to [Open].
type stateHalfOpen[T any] struct{}

func (st *stateHalfOpen[T]) State() State[T]                  { return State[T](StateHalfOpen) }
func (st *stateHalfOpen[T]) onEntry(cb *CircuitBreaker[T])    { cb.cnt.resetSuccesses() }
func (st *stateHalfOpen[T]) onExit(cb *CircuitBreaker[T])     {}
func (st *stateHalfOpen[T]) ready(cb *CircuitBreaker[T]) bool { return true }
func (st *stateHalfOpen[T]) onSuccess(cb *CircuitBreaker[T]) {
	if cb.cnt.Successes >= cb.halfOpenMaxSuccesses {
		cb.setState(&stateClosed[T]{})
	}
}
func (st *stateHalfOpen[T]) onFail(cb *CircuitBreaker[T]) {
	cb.setState(&stateOpen[T]{})
}
