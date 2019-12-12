package circuitbreaker_test

import (
	"testing"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/facebookgo/clock"
	"github.com/mercari/go-circuitbreaker"
	"github.com/stretchr/testify/assert"
)

func TestCircuitBreakerStateTransitions(t *testing.T) {
	clk := clock.NewMock()
	cb := circuitbreaker.New(circuitbreaker.WithShouldTrip(circuitbreaker.NewTripFuncThreshold(3)),
		circuitbreaker.WithClock(clk),
		circuitbreaker.WithTimeout(1000*time.Millisecond),
		circuitbreaker.WithHalfOpenMaxSuccesses(4))

	for i := 0; i < 10; i++ {
		// Scenario: 3 Fails. State changes to -> StateOpen.
		cb.Fail()
		assert.Equal(t, circuitbreaker.StateClosed, cb.State())
		cb.Fail()
		assert.Equal(t, circuitbreaker.StateClosed, cb.State())
		cb.Fail()
		assert.Equal(t, circuitbreaker.StateOpen, cb.State())

		// Scenario: After OpenTimeout exceeded. -> StateHalfOpen.
		assertChangeStateToHalfOpenAfter(t, cb, clk, 1000*time.Millisecond)

		// Scenario: Hit Fail. State back to StateOpen.
		cb.Fail()
		assert.Equal(t, circuitbreaker.StateOpen, cb.State())

		// Scenario: After OpenTimeout exceeded. -> StateHalfOpen. (again)
		assertChangeStateToHalfOpenAfter(t, cb, clk, 1000*time.Millisecond)

		// Scenario: Hit Success. State -> StateClosed.
		cb.Success()
		assert.Equal(t, circuitbreaker.StateHalfOpen, cb.State())
		cb.Success()
		assert.Equal(t, circuitbreaker.StateHalfOpen, cb.State())
		cb.Success()
		assert.Equal(t, circuitbreaker.StateHalfOpen, cb.State())
		cb.Success()
		assert.Equal(t, circuitbreaker.StateClosed, cb.State())
	}
}

// TestStateClosed tests...
// - Ready() always returns true.
// - Change state if Failures threshold reached.
// - Interval ticker reset the internal counter..
func TestStateClosed(t *testing.T) {
	clk := clock.NewMock()
	cb := circuitbreaker.New(circuitbreaker.WithShouldTrip(circuitbreaker.NewTripFuncThreshold(3)),
		circuitbreaker.WithClock(clk),
		circuitbreaker.WithInterval(1000*time.Millisecond))

	t.Run("Ready", func(t *testing.T) {
		assert.True(t, cb.Ready())
	})

	t.Run("open-if-shouldtrip-reached", func(t *testing.T) {
		cb.Reset()
		cb.Fail()
		cb.Fail()
		assert.Equal(t, circuitbreaker.StateClosed, cb.State())
		cb.Fail()
		assert.Equal(t, circuitbreaker.StateOpen, cb.State())
	})

	t.Run("ticker-reset-the-counter", func(t *testing.T) {
		cb.Reset()
		cb.Success()
		cb.Fail()
		clk.Add(999 * time.Millisecond)
		assert.Equal(t, circuitbreaker.Counters{Successes: 1, Failures: 1, ConsecutiveFailures: 1}, cb.Counters())
		clk.Add(1 * time.Millisecond)
		assert.Equal(t, circuitbreaker.Counters{}, cb.Counters())
	})
}

// TestStateOpen tests...
// - Ready() always returns false.
// - Change state to StateHalfOpen after timer.
func TestStateOpen(t *testing.T) {
	clk := clock.NewMock()
	cb := circuitbreaker.New(circuitbreaker.WithShouldTrip(circuitbreaker.NewTripFuncThreshold(3)),
		circuitbreaker.WithClock(clk),
		circuitbreaker.WithTimeout(500*time.Millisecond))
	t.Run("Ready", func(t *testing.T) {
		cb.SetState(circuitbreaker.StateOpen)
		assert.False(t, cb.Ready())
	})
	t.Run("HalfOpen-when-timer-triggered", func(t *testing.T) {
		cb.SetState(circuitbreaker.StateOpen)
		cb.Fail()
		cb.Success()

		clk.Add(499 * time.Millisecond)
		assert.Equal(t, circuitbreaker.StateOpen, cb.State())

		clk.Add(1 * time.Millisecond)
		assert.Equal(t, circuitbreaker.StateHalfOpen, cb.State())
		assert.Equal(t, circuitbreaker.Counters{Failures: 1}, cb.Counters()) // successes reset.
	})
	t.Run("HalfOpen-with-ExponentialOpenBackOff", func(t *testing.T) {
		clkMock := clock.NewMock()
		backoffTest := &backoff.ExponentialBackOff{
			InitialInterval:     1000 * time.Millisecond,
			RandomizationFactor: 0,
			Multiplier:          2,
			MaxInterval:         5 * time.Second,
			MaxElapsedTime:      0,
			Clock:               clkMock,
		}
		cb := circuitbreaker.New(circuitbreaker.WithShouldTrip(circuitbreaker.NewTripFuncThreshold(1)),
			circuitbreaker.WithHalfOpenMaxSuccesses(1),
			circuitbreaker.WithClock(clkMock),
			circuitbreaker.WithBackOff(backoffTest))
		backoffTest.Reset()

		tests := []struct {
			f     func()
			after time.Duration
		}{
			{f: cb.Fail, after: 1000 * time.Millisecond},
			{f: cb.Fail, after: 2000 * time.Millisecond},
			{f: cb.Fail, after: 4000 * time.Millisecond},
			{f: cb.Fail, after: 5000 * time.Millisecond},
			{f: func() { cb.Success(); cb.Fail() }, after: 1000 * time.Millisecond},
		}
		for _, test := range tests {
			test.f()
			assert.Equal(t, circuitbreaker.StateOpen, cb.State())

			clkMock.Add(test.after - 1)
			assert.Equal(t, circuitbreaker.StateOpen, cb.State())

			clkMock.Add(1)
			assert.Equal(t, circuitbreaker.StateHalfOpen, cb.State())
		}
	})
	t.Run("OpenBackOff", func(t *testing.T) {
		clkMock := clock.NewMock()
		backoffTest := &backoff.ExponentialBackOff{
			InitialInterval:     1000 * time.Millisecond,
			RandomizationFactor: 0,
			Multiplier:          2,
			MaxInterval:         5 * time.Second,
			MaxElapsedTime:      0,
			Clock:               clkMock,
		}
		cb := circuitbreaker.New(circuitbreaker.WithShouldTrip(circuitbreaker.NewTripFuncThreshold(1)),
			circuitbreaker.WithHalfOpenMaxSuccesses(1),
			circuitbreaker.WithClock(clkMock),
			circuitbreaker.WithBackOff(backoffTest))
		backoffTest.Reset()

		tests := []struct {
			f     func()
			after time.Duration
		}{
			{f: cb.Fail, after: 1000 * time.Millisecond},
			{f: cb.Fail, after: 2000 * time.Millisecond},
			{f: cb.Fail, after: 4000 * time.Millisecond},
			{f: cb.Fail, after: 5000 * time.Millisecond},
			{f: func() { cb.Success(); cb.Fail() }, after: 1000 * time.Millisecond},
		}
		for _, test := range tests {
			test.f()
			assertChangeStateToHalfOpenAfter(t, cb, clkMock, test.after)
		}
	})
}

func assertChangeStateToHalfOpenAfter(t *testing.T, cb *circuitbreaker.CircuitBreaker, clock *clock.Mock, after time.Duration) {
	clock.Add(after - 1)
	assert.Equal(t, circuitbreaker.StateOpen, cb.State())

	clock.Add(1)
	assert.Equal(t, circuitbreaker.StateHalfOpen, cb.State())
}

// StateOpen Test
// - Ready() always returns true.
// - If get a fail, the state changes to Open.
// - If get a success, the state changes to Closed.
func TestHalfOpen(t *testing.T) {
	clkMock := clock.NewMock()
	cb := circuitbreaker.New(circuitbreaker.WithShouldTrip(circuitbreaker.NewTripFuncThreshold(3)),
		circuitbreaker.WithClock(clkMock),
		circuitbreaker.WithHalfOpenMaxSuccesses(4))
	t.Run("Ready", func(t *testing.T) {
		cb.Reset()
		cb.SetState(circuitbreaker.StateHalfOpen)
		assert.True(t, cb.Ready())
	})
	t.Run("Open-if-got-a-fail", func(t *testing.T) {
		cb.Reset()
		cb.SetState(circuitbreaker.StateHalfOpen)

		cb.Fail()
		assert.Equal(t, circuitbreaker.StateOpen, cb.State())
		assert.Equal(t, circuitbreaker.Counters{Failures: 1, ConsecutiveFailures: 1}, cb.Counters()) // no reset
	})
	t.Run("Close-if-success-reaches-HalfOpenMaxSuccesses", func(t *testing.T) {
		cb.Reset()
		cb.Fail()
		cb.SetState(circuitbreaker.StateHalfOpen)

		cb.Success()
		cb.Success()
		cb.Success()
		assert.Equal(t, circuitbreaker.StateHalfOpen, cb.State())

		cb.Success()
		assert.Equal(t, circuitbreaker.StateClosed, cb.State())
		assert.Equal(t, circuitbreaker.Counters{Successes: 4, Failures: 0, ConsecutiveSuccesses: 4}, cb.Counters()) // Failures reset.
	})
}
