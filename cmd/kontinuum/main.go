// Package main is the entry point for kontinuum, a Kubernetes-style API
// server built on kommodity's libkapi.
package main

import (
	"errors"
	"os"

	"github.com/nicklasfrahm/kontinuum/pkg/cli"
)

func main() {
	err := cli.NewRootCmd().Execute()
	if err == nil {
		return
	}

	// A command may ask for a specific exit code, e.g. up --dry-run's 2
	// for pending changes; every other error exits 1.
	exitErr, ok := errors.AsType[*cli.ExitError](err)
	if ok {
		os.Exit(exitErr.Code)
	}

	os.Exit(1)
}
