package courier

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrClosed       = errors.New("courier: closed")
	ErrOverloaded   = errors.New("courier: capacity exceeded")
	ErrTooLarge     = errors.New("courier: message too large")
	ErrNoResponders = errors.New("courier: no responders")
)

// RPCError reports a remote framework failure, distinct from an IDL business exception.
// Is maps known codes to the corresponding sentinel or context error.
type RPCError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("courier %s: %s", e.Code, e.Message) }
func (e *RPCError) Is(target error) bool {
	switch e.Code {
	case "no_responders":
		return target == ErrNoResponders
	case "overloaded":
		return target == ErrOverloaded
	case "too_large":
		return target == ErrTooLarge
	case "deadline":
		return target == context.DeadlineExceeded
	}
	return false
}

// TransportError distinguishes publication failures from remote business errors.
type TransportError struct{ Cause error }

func (e *TransportError) Error() string { return "courier transport: " + e.Cause.Error() }
func (e *TransportError) Unwrap() error { return e.Cause }
