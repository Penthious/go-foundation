package main

import "testing"

func TestRun_ValidatesCommandBeforeOpeningDatabase(t *testing.T) {
	for _, args := range [][]string{nil, {"unknown"}, {"up", "extra"}} {
		if err := run(t.Context(), args); err == nil || err.Error() != "usage: migrate up|down|status|version" {
			t.Fatalf("invalid command result = %v", err)
		}
	}
}
