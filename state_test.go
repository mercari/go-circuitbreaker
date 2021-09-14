package circuitbreaker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/facebookgo/clock"
	"github.com/mercari/go-circuitbreaker"
	"github.com/stretchr/testify/assert"
)

func TestCircuitBreakerStateTransitions(t *testing.T) {
	clk := clock.NewMock()
	cb := circuitbreaker.New(circuitbreaker.WithTripFunc(circuitbreaker.NewTripFuncThreshold(3)),
		circuitbreaker.WithClock(clk),
		circuitbreaker.WithOpenTimeout(1000*time.Millisecond),
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

func TestCircuitBreakerOnStateChange(t *testing.T) {
	type stateChange struct {
		from circuitbreaker.State
		to   circuitbreaker.State
	}

	expectedStateChanges := []stateChange{
		{
			from: circuitbreaker.StateClosed,
			to:   circuitbreaker.StateOpen,
		},
		{
			from: circuitbreaker.StateOpen,
			to:   circuitbreaker.StateHalfOpen,
		},
		{
			from: circuitbreaker.StateHalfOpen,
			to:   circuitbreaker.StateOpen,
		},
		{
			from: circuitbreaker.StateOpen,
			to:   circuitbreaker.StateHalfOpen,
		},
		{
			from: circuitbreaker.StateHalfOpen,
			to:   circuitbreaker.StateClosed,
		},
	}
	var actualStateChanges []stateChange

	clock := clock.NewMock()
	cb := circuitbreaker.New(&circuitbreaker.Options{
		ShouldTrip:           circuitbreaker.NewTripFuncThreshold(3),
		Clock:                clock,
		OpenTimeout:          1000 * time.Millisecond,
		HalfOpenMaxSuccesses: 4,
		OnStateChange: func(from, to circuitbreaker.State) {
			actualStateChanges = append(actualStateChanges, stateChange{
				from: from,
				to:   to,
			})
		},
	})

	// Scenario: 3 Fails. State changes to -> StateOpen.
	cb.Fail()
	cb.Fail()
	cb.Fail()

	// Scenario: After OpenTimeout exceeded. -> StateHalfOpen.
	assertChangeStateToHalfOpenAfter(t, cb, clock, 1000*time.Millisecond)

	// Scenario: Hit Fail. State back to StateOpen.
	cb.Fail()

	// Scenario: After OpenTimeout exceeded. -> StateHalfOpen. (again)
	assertChangeStateToHalfOpenAfter(t, cb, clock, 1000*time.Millisecond)

	// Scenario: Hit Success. State -> StateClosed.
	cb.Success()
	cb.Success()
	cb.Success()
	cb.Success()

	assert.Equal(t, expectedStateChanges, actualStateChanges)
}

// TestStateClosed tests...
// - Ready() always returns true.
// - Change state if Failures threshold reached.
// - Interval ticker reset the internal counter..
func TestStateClosed(t *testing.T) {
	clk := clock.NewMock()
	cb := circuitbreaker.New(circuitbreaker.WithTripFunc(circuitbreaker.NewTripFuncThreshold(3)),
		circuitbreaker.WithClock(clk),
		circuitbreaker.WithCounterResetInterval(1000*time.Millisecond))

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
	cb := circuitbreaker.New(circuitbreaker.WithTripFunc(circuitbreaker.NewTripFuncThreshold(3)),
		circuitbreaker.WithClock(clk),
		circuitbreaker.WithOpenTimeout(500*time.Millisecond))
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
		cb := circuitbreaker.New(circuitbreaker.WithTripFunc(circuitbreaker.NewTripFuncThreshold(1)),
			circuitbreaker.WithHalfOpenMaxSuccesses(1),
			circuitbreaker.WithClock(clkMock),
			circuitbreaker.WithOpenTimeoutBackOff(backoffTest))
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
		cb := circuitbreaker.New(circuitbreaker.WithTripFunc(circuitbreaker.NewTripFuncThreshold(1)),
			circuitbreaker.WithHalfOpenMaxSuccesses(1),
			circuitbreaker.WithClock(clkMock),
			circuitbreaker.WithOpenTimeoutBackOff(backoffTest))
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
	cb := circuitbreaker.New(circuitbreaker.WithTripFunc(circuitbreaker.NewTripFuncThreshold(3)),
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

func run(wg *sync.WaitGroup, f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}

func TestRace(t *testing.T) {
	clock := clock.NewMock()
	cb := circuitbreaker.New(&circuitbreaker.Options{
		ShouldTrip: func(_ *circuitbreaker.Counters) bool { return true },
		Clock:      clock,
		Interval:   1000 * time.Millisecond,
	})
	wg := &sync.WaitGroup{}
	run(wg, func() {
		cb.SetState(circuitbreaker.StateClosed)
	})
	run(wg, func() {
		cb.Reset()
	})
	run(wg, func() {
		cb.Done(context.Background(), errors.New(""))
	})
	run(wg, func() {
		cb.Do(context.Background(), func() (interface{}, error) {
			return nil, nil
		})
	})
	run(wg, func() {
		cb.State()
	})
	run(wg, func() {
		cb.Fail()
	})
	run(wg, func() {
		cb.Counters()
	})
	run(wg, func() {
		cb.Ready()
	})
	run(wg, func() {
		cb.Success()
	})
	run(wg, func() {
		cb.FailWithContext(context.Background())
	})
	wg.Wait()
}
