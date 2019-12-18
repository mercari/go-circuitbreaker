package circuitbreaker_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/mercari/go-circuitbreaker"
	"github.com/stretchr/testify/assert"
)

type user struct {
	name string
	age  int
}

func fetchUserInfo(ctx context.Context, name string) (*user, error) {
	return &user{name: name, age: 30}, nil
}

func ExampleCircuitBreaker() {
	cb := circuitbreaker.New(nil)
	ctx := context.Background()

	data, err := cb.Do(context.Background(), func() (interface{}, error) {
		user, err := fetchUserInfo(ctx, "太郎")
		if err != nil && err.Error() == "UserNoFound" {
			// If you received a application level error, wrap it with Ignore to
			// avoid false-positive circuit open.
			return nil, circuitbreaker.Ignore(err)
		}
		return user, err
	})

	if err != nil {
		log.Fatalf("failed to fetch user:%s\n", err.Error())
	}
	log.Printf("fetched user:%+v\n", data.(*user))
}

func TestDo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cb := circuitbreaker.New(&circuitbreaker.Options{})
		got, err := cb.Do(context.Background(), func() (interface{}, error) {
			return "data", nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "data", got.(string))
		assert.Equal(t, int64(0), cb.Counters().Failures)
	})

	t.Run("error", func(t *testing.T) {
		cb := circuitbreaker.New(&circuitbreaker.Options{})
		wantErr := errors.New("something happens")
		got, err := cb.Do(context.Background(), func() (interface{}, error) {
			return "data", wantErr
		})
		assert.Equal(t, err, wantErr)
		assert.Equal(t, "data", got.(string))
		assert.Equal(t, int64(1), cb.Counters().Failures)
	})

	t.Run("ignore", func(t *testing.T) {
		cb := circuitbreaker.New(&circuitbreaker.Options{})
		wantErr := errors.New("something happens")
		got, err := cb.Do(context.Background(), func() (interface{}, error) { return "data", circuitbreaker.Ignore(wantErr) })
		assert.Equal(t, err, wantErr)
		assert.Equal(t, "data", got.(string))
		assert.Equal(t, int64(0), cb.Counters().Failures)
	})
	t.Run("markassuccess", func(t *testing.T) {
		cb := circuitbreaker.New(&circuitbreaker.Options{})
		wantErr := errors.New("something happens")
		got, err := cb.Do(context.Background(), func() (interface{}, error) { return "data", circuitbreaker.MarkAsSuccess(wantErr) })
		assert.Equal(t, err, wantErr)
		assert.Equal(t, "data", got.(string))
		assert.Equal(t, int64(0), cb.Counters().Failures)
	})

	t.Run("context-canceled", func(t *testing.T) {
		tests := []struct {
			FailOnContextCancel bool
			ExpectedFailures    int64
		}{
			{FailOnContextCancel: true, ExpectedFailures: 1},
			{FailOnContextCancel: false, ExpectedFailures: 0},
		}
		for _, test := range tests {
			cancelErr := errors.New("context's Done channel closed.")
			t.Run(fmt.Sprintf("FailOnContextCanceled=%t", test.FailOnContextCancel), func(t *testing.T) {
				cb := circuitbreaker.New(&circuitbreaker.Options{
					FailOnContextCancel: test.FailOnContextCancel,
				})
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				got, err := cb.Do(ctx, func() (interface{}, error) {
					<-ctx.Done()
					return "", cancelErr
				})
				assert.Equal(t, err, cancelErr)
				assert.Equal(t, "", got.(string))
				assert.Equal(t, test.ExpectedFailures, cb.Counters().Failures)
			})
		}
	})

	t.Run("context-timeout", func(t *testing.T) {
		tests := []struct {
			FailOnContextDeadline bool
			ExpectedFailures      int64
		}{
			{FailOnContextDeadline: true, ExpectedFailures: 1},
			{FailOnContextDeadline: false, ExpectedFailures: 0},
		}
		for _, test := range tests {
			timeoutErr := errors.New("context's Done channel closed.")
			t.Run(fmt.Sprintf("FailOnContextDeadline=%t", test.FailOnContextDeadline), func(t *testing.T) {
				cb := circuitbreaker.New(&circuitbreaker.Options{
					FailOnContextDeadline: test.FailOnContextDeadline,
				})
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
				defer cancel()
				got, err := cb.Do(ctx, func() (interface{}, error) {
					<-ctx.Done()
					return "", timeoutErr
				})
				assert.Equal(t, err, timeoutErr)
				assert.Equal(t, "", got.(string))
				assert.Equal(t, test.ExpectedFailures, cb.Counters().Failures)
			})
		}
	})

	t.Run("cyclic-state-transition", func(t *testing.T) {
		clock := clock.NewMock()
		cb := circuitbreaker.New(&circuitbreaker.Options{
			ShouldTrip:           circuitbreaker.NewTripFuncThreshold(3),
			Clock:                clock,
			OpenTimeout:          1000 * time.Millisecond,
			HalfOpenMaxSuccesses: 4,
		})

		wantErr := errors.New("something happens")

		// ( Closed => Open => HalfOpen => Open => HalfOpen => Closed ) x 10 iterations.
		for i := 0; i < 10; i++ {

			// State: Closed.
			for i := 0; i < 3; i++ {
				assert.Equal(t, circuitbreaker.StateClosed, cb.State())
				got, err := cb.Do(context.Background(), func() (interface{}, error) { return "data", wantErr })
				assert.Equal(t, err, wantErr)
				assert.Equal(t, "data", got.(string))
			}

			// State: Closed => Open. Should return nil and ErrOpen error.
			assert.Equal(t, circuitbreaker.StateOpen, cb.State())
			got, err := cb.Do(context.Background(), func() (interface{}, error) { return "data", wantErr })
			assert.Equal(t, err, circuitbreaker.ErrOpen)
			assert.Nil(t, got)

			// State: Open => HalfOpen.
			clock.Add(1000 * time.Millisecond)
			assert.Equal(t, circuitbreaker.StateHalfOpen, cb.State())

			// State: HalfOpen => Open.
			got, err = cb.Do(context.Background(), func() (interface{}, error) { return "data", wantErr })
			assert.Equal(t, err, wantErr)
			assert.Equal(t, "data", got.(string))
			assert.Equal(t, circuitbreaker.StateOpen, cb.State())

			// State: Open => HalfOpen.
			clock.Add(1000 * time.Millisecond)

			// State: HalfOpen => Close.
			for i := 0; i < 4; i++ {
				assert.Equal(t, circuitbreaker.StateHalfOpen, cb.State())
				got, err = cb.Do(context.Background(), func() (interface{}, error) { return "data", nil })
				assert.Equal(t, err, nil)
				assert.Equal(t, "data", got.(string))
			}
			assert.Equal(t, circuitbreaker.StateClosed, cb.State())
		}
	})
}

func TestCircuitBreakerTripFuncs(t *testing.T) {
	t.Run("TripFuncThreshold", func(t *testing.T) {
		shouldTrip := circuitbreaker.NewTripFuncThreshold(5)
		assert.False(t, shouldTrip(&circuitbreaker.Counters{Failures: 4}))
		assert.True(t, shouldTrip(&circuitbreaker.Counters{Failures: 5}))
		assert.True(t, shouldTrip(&circuitbreaker.Counters{Failures: 6}))
	})
	t.Run("TripFuncConsecutiveFailures", func(t *testing.T) {
		shouldTrip := circuitbreaker.NewTripFuncConsecutiveFailures(5)
		assert.False(t, shouldTrip(&circuitbreaker.Counters{ConsecutiveFailures: 4}))
		assert.True(t, shouldTrip(&circuitbreaker.Counters{ConsecutiveFailures: 5}))
		assert.True(t, shouldTrip(&circuitbreaker.Counters{ConsecutiveFailures: 6}))
	})
	t.Run("TripFuncFailureRate", func(t *testing.T) {
		shouldTrip := circuitbreaker.NewTripFuncFailureRate(10, 0.4)
		assert.False(t, shouldTrip(&circuitbreaker.Counters{Successes: 1, Failures: 8}))
		assert.True(t, shouldTrip(&circuitbreaker.Counters{Successes: 1, Failures: 9}))
		assert.False(t, shouldTrip(&circuitbreaker.Counters{Successes: 60, Failures: 39}))
		assert.True(t, shouldTrip(&circuitbreaker.Counters{Successes: 60, Failures: 40}))
		assert.True(t, shouldTrip(&circuitbreaker.Counters{Successes: 60, Failures: 41}))
	})
}

func TestIgnore(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		assert.Nil(t, circuitbreaker.Ignore(nil))
	})
	t.Run("ignore", func(t *testing.T) {
		originalErr := errors.New("logic error")
		err := circuitbreaker.Ignore(originalErr)
		assert.Equal(t, err.Error(), "circuitbreaker does not mark this error as a failure: logic error")
		nfe, ok := err.(*circuitbreaker.IgnorableError)
		assert.True(t, ok)
		assert.Equal(t, nfe.Unwrap(), originalErr)
	})
}

func TestMarkAsSuccess(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		assert.Nil(t, circuitbreaker.MarkAsSuccess(nil))
	})
	t.Run("MarkAsSuccess", func(t *testing.T) {
		originalErr := errors.New("logic error")
		err := circuitbreaker.MarkAsSuccess(originalErr)
		assert.Equal(t, err.Error(), "circuitbreaker mark this error as a success: logic error")
		nfe, ok := err.(*circuitbreaker.SuccessMarkableError)
		assert.True(t, ok)
		assert.Equal(t, nfe.Unwrap(), originalErr)
	})
}

func TestSuccess(t *testing.T) {
	cb := circuitbreaker.New(nil)
	cb.Success()
	assert.Equal(t, circuitbreaker.Counters{Successes: 1, Failures: 0, ConsecutiveSuccesses: 1, ConsecutiveFailures: 0}, cb.Counters())

	// Test if Success resets ConsecutiveFailures.
	cb.Fail()
	cb.Success()
	assert.Equal(t, circuitbreaker.Counters{Successes: 2, Failures: 1, ConsecutiveSuccesses: 1, ConsecutiveFailures: 0}, cb.Counters())

}

func TestFail(t *testing.T) {
	cb := circuitbreaker.New(nil)
	cb.Fail()
	assert.Equal(t, circuitbreaker.Counters{Successes: 0, Failures: 1, ConsecutiveSuccesses: 0, ConsecutiveFailures: 1}, cb.Counters())

	// Test if Fail resets ConsecutiveSuccesses.
	cb.Success()
	cb.Fail()
	assert.Equal(t, circuitbreaker.Counters{Successes: 1, Failures: 2, ConsecutiveSuccesses: 0, ConsecutiveFailures: 1}, cb.Counters())
}

// TestReset tests if Reset resets all counters.
func TestReset(t *testing.T) {
	cb := circuitbreaker.New(nil)
	cb.Success()
	cb.Reset()
	assert.Equal(t, circuitbreaker.Counters{}, cb.Counters())

	cb.Fail()
	cb.Reset()
	assert.Equal(t, circuitbreaker.Counters{}, cb.Counters())
}

func TestReportFunctions(t *testing.T) {
	t.Run("Failed if ctx.Err() == nil", func(t *testing.T) {
		cb := circuitbreaker.New(nil)
		cb.FailWithContext(context.Background())
		assert.Equal(t, int64(1), cb.Counters().Failures)
	})
	t.Run("ctx.Err() == context.Canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		cb := circuitbreaker.New(nil)
		cb.FailWithContext(ctx)
		assert.Equal(t, int64(0), cb.Counters().Failures)

		cb = circuitbreaker.New(&circuitbreaker.Options{FailOnContextCancel: true})
		cb.FailWithContext(ctx)
		assert.Equal(t, int64(1), cb.Counters().Failures)
	})
	t.Run("ctx.Err() == context.DeadlineExceeded", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Time{})
		defer cancel()
		cb := circuitbreaker.New(nil)
		cb.FailWithContext(ctx)
		assert.Equal(t, int64(0), cb.Counters().Failures)

		cb = circuitbreaker.New(&circuitbreaker.Options{FailOnContextDeadline: true})
		cb.FailWithContext(ctx)
		assert.Equal(t, int64(1), cb.Counters().Failures)
	})
}

func TestNotifyStateChange(t *testing.T) {
	t.Run("Notify collectly", func(t *testing.T) {
		cb := circuitbreaker.New(&circuitbreaker.Options{NotifyStateChange: true})
		go func() {
			ch, _ := cb.GetStateChanges()
			var change circuitbreaker.StateChange
			change = <-ch
			assert.Equal(t, circuitbreaker.StateChange{circuitbreaker.StateClosed, circuitbreaker.StateOpen}, change)
			change = <-ch
			assert.Equal(t, circuitbreaker.StateChange{circuitbreaker.StateOpen, circuitbreaker.StateHalfOpen}, change)
			change = <-ch
			assert.Equal(t, circuitbreaker.StateChange{circuitbreaker.StateHalfOpen, circuitbreaker.StateClosed}, change)
		}()
		cb.SetState(circuitbreaker.StateOpen)
		cb.SetState(circuitbreaker.StateHalfOpen)
		cb.SetState(circuitbreaker.StateClosed)
	})
	t.Run("Not blocked", func(t *testing.T) {
		cb := circuitbreaker.New(&circuitbreaker.Options{NotifyStateChange: true})
		doneCh := make(chan struct{})
		resultCh := make(chan struct{})
		go func() {
			for {
				cb.Ready()
				cb.Success()
				select {
				case <-doneCh:
					close(resultCh)
					return
				default:
				}
			}
		}()
		for i := 0; i < 100; i++ {
			cb.SetState(circuitbreaker.StateClosed)
		}
		close(doneCh)
		<-resultCh
	})
}
