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
	t.Run("ConstantOpenTimeout", func(t *testing.T) {
		clock := clock.NewMock()
		cb := circuitbreaker.New(&circuitbreaker.Options{
			ShouldTrip:           circuitbreaker.NewTripFuncThreshold(3),
			Clock:                clock,
			OpenTimeout:          1000 * time.Millisecond,
			HalfOpenMaxSuccesses: 4,
		})
		assertState(t, cb, circuitbreaker.StateClosed, &circuitbreaker.Counters{})

		// Scenario: 3 Fails. State changes to -> StateOpen.
		doAndAssertState(t, cb, cb.Success, circuitbreaker.StateClosed, &circuitbreaker.Counters{Successes: 1, ConsecutiveSuccesses: 1})
		doAndAssertState(t, cb, cb.Success, circuitbreaker.StateClosed, &circuitbreaker.Counters{Successes: 2, ConsecutiveSuccesses: 2})
		doAndAssertState(t, cb, cb.Success, circuitbreaker.StateClosed, &circuitbreaker.Counters{Successes: 3, ConsecutiveSuccesses: 3})
		doAndAssertState(t, cb, cb.Success, circuitbreaker.StateClosed, &circuitbreaker.Counters{Successes: 4, ConsecutiveSuccesses: 4})
		doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateClosed, &circuitbreaker.Counters{Successes: 4, Failures: 1, ConsecutiveFailures: 1})
		doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateClosed, &circuitbreaker.Counters{Successes: 4, Failures: 2, ConsecutiveFailures: 2})
		doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateOpen, &circuitbreaker.Counters{Successes: 4, Failures: 3, ConsecutiveFailures: 3})

		// Scenario: After OpenTimeout exceeded. -> StateHalfOpen.
		clock.Add(999 * time.Millisecond)
		assertState(t, cb, circuitbreaker.StateOpen, &circuitbreaker.Counters{Successes: 4, Failures: 3, ConsecutiveFailures: 3}) // not yet.
		clock.Add(1 * time.Millisecond)
		assertState(t, cb, circuitbreaker.StateHalfOpen, &circuitbreaker.Counters{Failures: 3, ConsecutiveFailures: 3})

		// Scenario: Hit Fail. State back to StateOpen.
		doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateOpen, &circuitbreaker.Counters{Failures: 4, ConsecutiveFailures: 4})

		// Scenario: After OpenTimeout exceeded. -> StateHalfOpen. (again)
		clock.Add(1000 * time.Millisecond)
		assertState(t, cb, circuitbreaker.StateHalfOpen, &circuitbreaker.Counters{Failures: 4, ConsecutiveFailures: 4})

		// Scenario: Hit Success. State -> StateClosed.
		doAndAssertState(t, cb, cb.Success, circuitbreaker.StateHalfOpen, &circuitbreaker.Counters{Successes: 1, Failures: 4, ConsecutiveSuccesses: 1})
		doAndAssertState(t, cb, cb.Success, circuitbreaker.StateHalfOpen, &circuitbreaker.Counters{Successes: 2, Failures: 4, ConsecutiveSuccesses: 2})
		doAndAssertState(t, cb, cb.Success, circuitbreaker.StateHalfOpen, &circuitbreaker.Counters{Successes: 3, Failures: 4, ConsecutiveSuccesses: 3})
		doAndAssertState(t, cb, cb.Success, circuitbreaker.StateClosed, &circuitbreaker.Counters{Successes: 4, Failures: 0, ConsecutiveSuccesses: 4})
	})

	t.Run("WithOpenBackOff", func(t *testing.T) {
		clock := clock.NewMock()
		backoff := &backoff.ExponentialBackOff{
			InitialInterval:     1000 * time.Millisecond,
			RandomizationFactor: 0,
			Multiplier:          2,
			MaxInterval:         10 * time.Second,
			MaxElapsedTime:      0,
			Clock:               clock,
		}
		backoff.Reset()

		cb := circuitbreaker.New(&circuitbreaker.Options{
			ShouldTrip:           circuitbreaker.NewTripFuncThreshold(1),
			HalfOpenMaxSuccesses: 1,
			Clock:                clock,
			OpenBackOff:          backoff,
		})

		assert := func(after time.Duration) {
			t.Helper()

			assertState(t, cb, circuitbreaker.StateOpen, nil)
			clock.Add(after - 1)
			assertState(t, cb, circuitbreaker.StateOpen, nil)
			clock.Add(1)
			assertState(t, cb, circuitbreaker.StateHalfOpen, nil)
		}

		assertState(t, cb, circuitbreaker.StateClosed, nil)

		cb.Fail()
		assert(1000 * time.Millisecond) // InitialInterval

		cb.Fail()
		assert(2000 * time.Millisecond) // InitialInterval * Multiplier

		cb.Fail()
		assert(4000 * time.Millisecond) // InitialInterval * (Multiplier ^ 2)

		cb.Fail()
		assert(8000 * time.Millisecond) // InitialInterval * (Multiplier ^ 3)

		cb.Fail()
		assert(10000 * time.Millisecond) // capped by MaxInterval

		// cb.Success Reset the backoff.
		doAndAssertState(t, cb, cb.Success, circuitbreaker.StateClosed, nil)

		cb.Fail()
		assert(1000 * time.Millisecond) // capped by MaxInterval
	})
}

func TestCircuitBreakerState(t *testing.T) {
	clock := clock.NewMock()
	cb := circuitbreaker.New(&circuitbreaker.Options{
		ShouldTrip:           circuitbreaker.NewTripFuncThreshold(3),
		Clock:                clock,
		Interval:             1000 * time.Millisecond,
		OpenTimeout:          500 * time.Millisecond,
		HalfOpenMaxSuccesses: 4,
	})

	t.Run("StateClosed", func(t *testing.T) {
		// StateClosed Test
		// - Ready() always returns true.
		// - Change state when Failure threshold reached.
		// - Interval ticker reset the internal counter..

		t.Run("ready()-returns-true", func(t *testing.T) {
			cb.Reset()
			assert.True(t, cb.Ready())
		})

		t.Run("change-state-when-failure-threshold-reached", func(t *testing.T) {
			cb.Reset()
			doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateClosed, &circuitbreaker.Counters{Failures: 1, ConsecutiveFailures: 1}) // 1st fail.
			doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateClosed, &circuitbreaker.Counters{Failures: 2, ConsecutiveFailures: 2}) // 2nc fail.

			// successes does not change the state...
			for i := int64(1); i <= 10; i++ {
				doAndAssertState(t, cb, cb.Success, circuitbreaker.StateClosed, &circuitbreaker.Counters{Successes: i, Failures: 2, ConsecutiveSuccesses: i}) // 1st fail.
			}
			// 3rd fail. should change the state to Open.
			doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateOpen, &circuitbreaker.Counters{Successes: 10, Failures: 3, ConsecutiveFailures: 1})
		})

		t.Run("ticker-reset-the-counter", func(t *testing.T) {
			cb.Reset()
			doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateClosed, &circuitbreaker.Counters{Failures: 1, ConsecutiveFailures: 1})
			doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateClosed, &circuitbreaker.Counters{Failures: 2, ConsecutiveFailures: 2})
			clock.Add(1200 * time.Millisecond)
			assertState(t, cb, circuitbreaker.StateClosed, &circuitbreaker.Counters{}) // failures is reset after the interval.
		})
	})

	t.Run("StateOpen", func(t *testing.T) {
		// StateOpen Test
		// - Ready() always returns false.
		// - Change state to StateHalfOpen after timer.

		t.Run("ready()-returns-false", func(t *testing.T) {
			cb.SetState(circuitbreaker.StateOpen)
			assert.False(t, cb.Ready())
		})
		t.Run("change-state-when-timer-triggered", func(t *testing.T) {
			cb.SetState(circuitbreaker.StateOpen)
			clock.Add(499 * time.Millisecond)
			// Successes / Failures does not affect the internal state...
			for i := int64(1); i <= 10; i++ {
				doAndAssertState(t, cb, cb.Success, circuitbreaker.StateOpen, &circuitbreaker.Counters{Successes: i, ConsecutiveSuccesses: i})
			}
			for i := int64(1); i <= 10; i++ {
				doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateOpen, &circuitbreaker.Counters{Successes: 10, Failures: i, ConsecutiveFailures: i})
			}
			clock.Add(1 * time.Millisecond)
			assertState(t, cb, circuitbreaker.StateHalfOpen, &circuitbreaker.Counters{Successes: 0, Failures: 10, ConsecutiveFailures: 10})
		})
	})

	t.Run("StateHalfOpen", func(t *testing.T) {
		// StateOpen Test
		// - Ready() always returns true.
		// - If get a fail, the state changes to Open.
		// - If get a success, the state changes to Closed.

		t.Run("ready()-returns-true", func(t *testing.T) {
			cb.Reset()
			cb.SetState(circuitbreaker.StateHalfOpen)
			assert.True(t, cb.Ready())
		})
		t.Run("change-state-to-StateOpen-if-got-a-fail", func(t *testing.T) {
			cb.Reset()
			cb.SetState(circuitbreaker.StateHalfOpen)
			doAndAssertState(t, cb, cb.Fail, circuitbreaker.StateOpen, &circuitbreaker.Counters{Failures: 1, ConsecutiveFailures: 1})
		})
		t.Run("change-state-to-StateClosed-if-got-a-success", func(t *testing.T) {
			cb.Reset()
			cb.SetState(circuitbreaker.StateHalfOpen)
			doAndAssertState(t, cb, cb.Success, circuitbreaker.StateHalfOpen, &circuitbreaker.Counters{Successes: 1, ConsecutiveSuccesses: 1})
			doAndAssertState(t, cb, cb.Success, circuitbreaker.StateHalfOpen, &circuitbreaker.Counters{Successes: 2, ConsecutiveSuccesses: 2})
			doAndAssertState(t, cb, cb.Success, circuitbreaker.StateHalfOpen, &circuitbreaker.Counters{Successes: 3, ConsecutiveSuccesses: 3})
			doAndAssertState(t, cb, cb.Success, circuitbreaker.StateClosed, &circuitbreaker.Counters{Successes: 4, ConsecutiveSuccesses: 4})
		})
	})
}
