// Package protocol holds the types generated from protocol/openapi.yaml.
package protocol

//go:generate go tool oapi-codegen -config oapi-codegen.yaml -o openapi.gen.go ../../protocol/openapi.yaml
//go:generate go run ../../protocol/wsgen -spec ../../protocol/openapi.yaml -out ws.gen.go
