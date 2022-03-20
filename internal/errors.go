package internal

import "errors"

var (
	ErrIndexNotFound  = errors.New("index not found")
	ErrIndexReadError = errors.New("index read error")
)
