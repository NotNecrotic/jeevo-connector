// Package version holds the build version, set via -ldflags -X at release.
package version

// Version is the connector release version (semver, no leading "v").
var Version = "0.0.0-dev"

// Commit is the source revision the build came from, when known.
var Commit = ""
