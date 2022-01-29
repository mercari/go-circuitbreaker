package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	backoff "github.com/cenkalti/backoff/v3"
)

var (
	// ErrOpen is an error to signify that the CB is open and executing
	// operations are not allowed.
	ErrOpen = errors.New("circuit breaker open")

	// DefaultTripFunc is used when Options.ShouldTrip is nil.
	DefaultTripFunc = NewTripFuncThreshold(10)
)

// Default setting parameters.
const (
	DefaultInterval             = 1 * time.Second
	DefaultHalfOpenMaxSuccesses = 4
)

// State represents the internal state of CB.
type State string

// State constants.
const (
	StateClosed   State = "closed"
	StateOpen     State = "open"
	StateHalfOpen State = "half-open"
)

// DefaultOpenBackOff returns defaultly used BackOff.
func DefaultOpenBackOff() backoff.BackOff {
	_backoff := backoff.NewExponentialBackOff()
	_backoff.MaxElapsedTime = 0
	_backoff.Reset()
	return _backoff
}

// Counters holds internal counter(s) of CircuitBreaker.
type Counters struct {
	Successes            int64
	Failures             int64
	ConsecutiveSuccesses int64
	ConsecutiveFailures  int64
}

func (c *Counters) reset() { *c = Counters{} }

func (c *Counters) resetSuccesses() {
	c.Successes = 0
	c.ConsecutiveSuccesses = 0
}

func (c *Counters) resetFailures() {
	c.Failures = 0
	c.ConsecutiveFailures = 0
}

func (c *Counters) incrementSuccesses() {
	c.Successes++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

func (c *Counters) incrementFailures() {
	c.Failures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

// StateChangeHook is a function which will be invoked when the state is changed.
type StateChangeHook func(oldState, newState State)

// TripFunc is a function to determine if CircuitBreaker should open (trip) or
// not. TripFunc is called when cb.Fail was called and the state was
// StateClosed. If TripFunc returns true, the cb's state goes to StateOpen.
type TripFunc func(*Counters) bool

// NewTripFuncThreshold provides a TripFunc. It returns true if the
// Failures counter is larger than or equals to threshold.
func NewTripFuncThreshold(threshold int64) TripFunc {
	return func(cnt *Counters) bool { return cnt.Failures >= threshold }
}

// NewTripFuncConsecutiveFailures provides a TripFunc that returns true
// if the consecutive failures is larger than or equals to threshold.
func NewTripFuncConsecutiveFailures(threshold int64) TripFunc {
	return func(cnt *Counters) bool { return cnt.ConsecutiveFailures >= threshold }
}

// NewTripFuncFailureRate provides a TripFunc that returns true if the failure
// rate is higher or equals to rate. If the samples are fewer than min, always
// returns false.
func NewTripFuncFailureRate(min int64, rate float64) TripFunc {
	return func(cnt *Counters) bool {
		if cnt.Successes+cnt.Failures < min {
			return false
		}
		return float64(cnt.Failures)/float64(cnt.Successes+cnt.Failures) >= rate
	}
}

// IgnorableError signals that the operation should not be marked as a failure.
type IgnorableError struct {
	err error
}

func (e *IgnorableError) Error() string {
	return fmt.Sprintf("circuitbreaker does not mark this error as a failure: %s", e.err.Error())
}

// Unwrap unwraps e.
func (e *IgnorableError) Unwrap() error { return e.err }

// Ignore wraps the given err in a *IgnorableError.
func Ignore(err error) error {
	if err == nil {
		return nil
	}
	return &IgnorableError{err}
}

// SuccessMarkableError signals that the operation should be mark as success.
type SuccessMarkableError struct {
	err error
}

func (e *SuccessMarkableError) Error() string {
	return fmt.Sprintf("circuitbreaker mark this error as a success: %s", e.err.Error())
}

// Unwrap unwraps e.
func (e *SuccessMarkableError) Unwrap() error { return e.err }

// MarkAsSuccess wraps the given err in a *SuccessMarkableError.
func MarkAsSuccess(err error) error {
	if err == nil {
		return nil
	}
	return &SuccessMarkableError{err}
}

// Options holds CircuitBreaker configuration options.
type options struct {
	// Clock to be used by CircuitBreaker. If nil, real-time clock is
	// used.
	clock clock.Clock

	// Interval is the cyclic time period to reset the internal counters
	// during state is in StateClosed.
	//
	// If zero, DefaultInterval is used. If Interval < 0, No interval will
	// be triggered.
	interval time.Duration

	// OpenTimeout is the period of StateOpened. After OpenTimeout,
	// CircuitBreaker's state will be changed to StateHalfOpened. If OpenBackOff
	// is not nil, OpenTimeout is ignored.
	openTimeout time.Duration

	// OpenBackOff is a Backoff to determine the period of StateOpened. Every
	// time the state transitions to StateOpened, OpenBackOff.NextBackOff()
	// recalculates the period. When the state transitions to StateClosed,
	// OpenBackOff is reset to the initial state. If both OpenTimeout is zero
	// value and OpenBackOff is empty, return value of DefaultOpenBackOff() is
	// used.
	//
	// NOTE: Please make sure not to set the ExponentialBackOff.MaxElapsedTime >
	// 0 for OpenBackOff. If so, your CB don't close after your period of the
	// StateOpened gets longer than the MaxElapsedTime.
	openBackOff backoff.BackOff

	// HalfOpenMaxSuccesses is max count of successive successes during the state
	// is in StateHalfOpened. If the state is StateHalfOpened and the successive
	// successes reaches this threshold, the state of CircuitBreaker changes
	// into StateClosed. If zero, DefaultHalfOpenMaxSuccesses is used.
	halfOpenMaxSuccesses int64

	// ShouldTrips is a function to determine if the CircuitBreaker should
	// trip. If the state is StateClosed and ShouldTrip returns true,
	// the state will be changed to StateOpened.
	// If nil, DefaultTripFunc is used.
	shouldTrip TripFunc

	// OnStateChange is a function which will be invoked when the state is changed.
	onStateChange StateChangeHook

	// FailOnContextCancel controls if CircuitBreaker mark an error when the
	// passed context.Done() is context.Canceled as a fail.
	failOnContextCancel bool

	// FailOnContextDeadline controls if CircuitBreaker mark an error when the
	// passed context.Done() is context.DeadlineExceeded as a fail.
	failOnContextDeadline bool
}

// CircuitBreaker provides circuit breaker pattern.
type CircuitBreaker struct {
	clock                 clock.Clock
	interval              time.Duration
	halfOpenMaxSuccesses  int64
	openBackOff           backoff.BackOff
	shouldTrip            TripFunc
	onStateChange         StateChangeHook
	failOnContextCancel   bool
	failOnContextDeadline bool

	mu    sync.RWMutex
	state state
	cnt   Counters
}

type fnApplyOptions func(*options)

// BreakerOption interface for applying configuration in the constructor
type BreakerOption interface {
	apply(*options)
}

func (f fnApplyOptions) apply(options *options) {
	f(options)
}

// WithTripFunc Set the function for counter
func WithTripFunc(tripFunc TripFunc) BreakerOption {
	return fnApplyOptions(func(options *options) {
		options.shouldTrip = tripFunc
	})
}

// WithClock Set the clock
func WithClock(clock clock.Clock) BreakerOption {
	return fnApplyOptions(func(options *options) {
		options.clock = clock
	})
}

// WithOpenTimeoutBackOff Set the time backoff
func WithOpenTimeoutBackOff(backoff backoff.BackOff) BreakerOption {
	return fnApplyOptions(func(options *options) {
		options.openBackOff = backoff
	})
}

// WithOpenTimeout Set the timeout of the circuit breaker
func WithOpenTimeout(timeout time.Duration) BreakerOption {
	return fnApplyOptions(func(options *options) {
		options.openTimeout = timeout
	})
}

// WithHalfOpenMaxSuccesses Set the number of half open successes
func WithHalfOpenMaxSuccesses(maxSuccesses int64) BreakerOption {
	return fnApplyOptions(func(options *options) {
		options.halfOpenMaxSuccesses = maxSuccesses
	})
}

// WithCounterResetInterval Set the interval of the circuit breaker, which is the cyclic time period to reset the internal counters
func WithCounterResetInterval(interval time.Duration) BreakerOption {
	return fnApplyOptions(func(options *options) {
		options.interval = interval
	})
}

// WithFailOnContextCancel Set if the context should fail on cancel
func WithFailOnContextCancel(failOnContextCancel bool) BreakerOption {
	return fnApplyOptions(func(options *options) {
		options.failOnContextCancel = failOnContextCancel
	})
}

// WithFailOnContextDeadline Set if the context should fail on deadline
func WithFailOnContextDeadline(failOnContextDeadline bool) BreakerOption {
	return fnApplyOptions(func(options *options) {
		options.failOnContextDeadline = failOnContextDeadline
	})
}

// WithOnStateChangeHookFn set a hook function that trigger if the condition of the StateChangeHook is true
func WithOnStateChangeHookFn(hookFn StateChangeHook) BreakerOption {
	return fnApplyOptions(func(options *options) {
		options.onStateChange = hookFn
	})
}

func defaultOptions() *options {
	return &options{
		shouldTrip:           DefaultTripFunc,
		clock:                clock.New(),
		openBackOff:          DefaultOpenBackOff(),
		openTimeout:          0,
		halfOpenMaxSuccesses: DefaultHalfOpenMaxSuccesses,
		interval:             DefaultInterval,
	}
}

// New returns a new CircuitBreaker
// The constructor will be instanced using the functional options pattern. When creating a new circuit breaker
// we should pass or left it blank if we want to use its default options.
// An example of the constructor would be like this:
//
// cb := circuitbreaker.New(
//     circuitbreaker.WithClock(clock.New()),
//     circuitbreaker.WithFailOnContextCancel(true),
//     circuitbreaker.WithFailOnContextDeadline(true),
//     circuitbreaker.WithHalfOpenMaxSuccesses(10),
//     circuitbreaker.WithOpenTimeoutBackOff(backoff.NewExponentialBackOff()),
//     circuitbreaker.WithOpenTimeout(10*time.Second),
//     circuitbreaker.WithCounterResetInterval(10*time.Second),
//     // we also have NewTripFuncThreshold and NewTripFuncConsecutiveFailures
//     circuitbreaker.WithTripFunc(circuitbreaker.NewTripFuncFailureRate(10, 0.4)),
//     circuitbreaker.WithOnStateChangeHookFn(func(from, to circuitbreaker.State) {
//       log.Printf("state changed from %s to %s\n", from, to)
// 	}),
// )
//
// The default options are described in the defaultOptions function
func New(opts ...BreakerOption) *CircuitBreaker {
	cbOptions := defaultOptions()

	for _, opt := range opts {
		opt.apply(cbOptions)
	}

	if cbOptions.openTimeout > 0 {
		cbOptions.openBackOff = backoff.NewConstantBackOff(cbOptions.openTimeout)
	}

	cb := &CircuitBreaker{
		shouldTrip:            cbOptions.shouldTrip,
		onStateChange:         cbOptions.onStateChange,
		clock:                 cbOptions.clock,
		interval:              cbOptions.interval,
		openBackOff:           cbOptions.openBackOff,
		halfOpenMaxSuccesses:  cbOptions.halfOpenMaxSuccesses,
		failOnContextCancel:   cbOptions.failOnContextCancel,
		failOnContextDeadline: cbOptions.failOnContextDeadline,
	}
	cb.setState(&stateClosed{})
	return cb
}

// An Operation is executed by Do().
type Operation func() (interface{}, error)

// Do executes the Operation o and returns the return values if
// cb.Ready() is true. If not ready, cb doesn't execute f and returns
// ErrOpen.
//
// If o returns a nil-error, cb counts the execution of Operation as a
// success. Otherwise, cb count it as a failure.
//
// If o returns a *IgnorableError, Do() ignores the result of operation and
// returns the wrapped error.
//
// If o returns a *SuccessMarkableError, Do() count it as a success and returns
// the wrapped error.
//
// If given Options' FailOnContextCancel is false (default), cb.Do
// doesn't mark the Operation's error as a failure if ctx.Err() returns
// context.Canceled.
//
// If given Options' FailOnContextDeadline is false (default), cb.Do
// doesn't mark the Operation's error as a failure if ctx.Err() returns
// context.DeadlineExceeded.
func (cb *CircuitBreaker) Do(ctx context.Context, o Operation) (interface{}, error) {
	if !cb.Ready() {
		return nil, ErrOpen
	}
	result, err := o()
	return result, cb.Done(ctx, err)
}

// Ready reports if cb is ready to execute an operation. Ready does not give
// any change to cb.
func (cb *CircuitBreaker) Ready() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state.ready(cb)
}

// Success signals that an execution of operation has been completed
// successfully to cb.
func (cb *CircuitBreaker) Success() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.cnt.incrementSuccesses()
	cb.state.onSuccess(cb)
}

// Fail signals that an execution of operation has been failed to cb.
func (cb *CircuitBreaker) Fail() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.cnt.incrementFailures()
	cb.state.onFail(cb)
}

// FailWithContext calls Fail internally. But if FailOnContextCancel is false
// and ctx is done with context.Canceled error, no Fail() called. Similarly, if
// FailOnContextDeadline is false and ctx is done with context.DeadlineExceeded
// error, no Fail() called.
func (cb *CircuitBreaker) FailWithContext(ctx context.Context) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		if ctxErr == context.Canceled && !cb.failOnContextCancel {
			return
		}
		if ctxErr == context.DeadlineExceeded && !cb.failOnContextDeadline {
			return
		}
	}
	cb.Fail()
}

// Done is a helper function to finish the protected operation. If err is nil,
// Done calls Success and returns nil. If err is a SuccessMarkableError or
// IgnorableError, Done returns wrapped error. Otherwise, Done calls
// FailWithContext internally.
func (cb *CircuitBreaker) Done(ctx context.Context, err error) error {
	if err == nil {
		cb.Success()
		return nil
	}

	if successMarkableErr, ok := err.(*SuccessMarkableError); ok {
		cb.Success()
		return successMarkableErr.Unwrap()
	}

	if ignorableErr, ok := err.(*IgnorableError); ok {
		return ignorableErr.Unwrap()
	}

	cb.FailWithContext(ctx)
	return err
}

// State reports the curent State of cb.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state.State()
}

// Counters returns internal counters. If current status is not
// StateClosed, returns zero value.
func (cb *CircuitBreaker) Counters() Counters {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.cnt
}

// Reset resets cb's state with StateClosed.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.cnt.reset()
	cb.setState(&stateClosed{})
}

// SetState set state of cb to st.
func (cb *CircuitBreaker) SetState(st State) {
	switch st {
	case StateClosed:
		cb.setStateWithLock(&stateClosed{})
	case StateOpen:
		cb.setStateWithLock(&stateOpen{})
	case StateHalfOpen:
		cb.setStateWithLock(&stateHalfOpen{})
	default:
		panic("undefined state")
	}
}

func (cb *CircuitBreaker) setStateWithLock(s state) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.setState(s)
}

func (cb *CircuitBreaker) setState(s state) {
	if cb.state != nil {
		cb.state.onExit(cb)
	}
	from := cb.state
	cb.state = s
	cb.state.onEntry(cb)
	cb.handleOnStateChange(from, s)
}

func (cb *CircuitBreaker) handleOnStateChange(from, to state) {
	if from == nil || cb.onStateChange == nil {
		return
	}
	cb.onStateChange(from.State(), to.State())
}
