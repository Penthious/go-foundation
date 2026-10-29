// Package schema owns the initial SQL shared by migration and runtime helpers.
package schema

import _ "embed"

//go:embed 001_create_inbox.sql
var inbox string

//go:embed 001_create_outbox.sql
var outbox string

//go:embed 001_failed_messages.sql
var failedMessages string

//go:embed 001_schedules.sql
var scheduler string

// Inbox returns the initial consumer deduplication schema.
func Inbox() string {
	return inbox
}

// Outbox returns the initial transactional publication schema.
func Outbox() string {
	return outbox
}

// FailedMessages returns the initial failed-delivery and replay schema.
func FailedMessages() string {
	return failedMessages
}

// Scheduler returns the initial job coordination schema.
func Scheduler() string {
	return scheduler
}
