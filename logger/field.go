package logger

import "time"

// Field is an application attribute. Use the helpers to construct fields.
// The zero value and fields with empty keys are ignored.
type Field struct {
	key   string
	value any
}

// String creates a string field.
func String(key, value string) Field {
	return Field{key: key, value: value}
}

// Bool creates a boolean field.
func Bool(key string, value bool) Field {
	return Field{key: key, value: value}
}

// Int creates an integer field.
func Int(key string, value int) Field {
	return Field{key: key, value: value}
}

// Int64 creates a 64-bit integer field.
func Int64(key string, value int64) Field {
	return Field{key: key, value: value}
}

// Duration creates a field containing a Go duration string, such as "42ms".
// For numeric milliseconds, use Int64("duration_ms", value.Milliseconds()).
func Duration(key string, value time.Duration) Field {
	return Field{key: key, value: value.String()}
}

// Err creates an "error" field containing err.Error(). A nil error is omitted.
func Err(err error) Field {
	if err == nil {
		return Field{}
	}

	return String("error", err.Error())
}
