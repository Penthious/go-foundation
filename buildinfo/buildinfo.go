// Package buildinfo exposes image metadata embedded by the Go linker.
package buildinfo

// Linker target names are a build API; retain constant string initializers for -X.
var version = "dev"
var commit = "unknown"
var buildTime = "unknown"

// Info describes the running executable, not the go-foundation dependency's
// release. BuildTime is an informational string, conventionally UTC RFC3339.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
}

// Current returns a copy of the executable's embedded build metadata. Values
// default to dev/unknown/unknown and can be set with -ldflags -X targeting the
// package's version, commit, and buildTime variables. It does not read the
// environment, invoke git, or change deployment configuration.
func Current() Info {
	return Info{Version: version, Commit: commit, BuildTime: buildTime}
}
