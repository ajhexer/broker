package broker

import "errors"

var (
	ErrUnavailable = errors.New("service is unavailable")
	ErrInvalidID   = errors.New("message with id provided is not valid or never published")
	ErrExpiredID   = errors.New("message with id provided is expired")
)
