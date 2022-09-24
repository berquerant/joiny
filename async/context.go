package async

import "context"

// Done returns true if context is canceled.
// This is non-blocking.
func Done(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
