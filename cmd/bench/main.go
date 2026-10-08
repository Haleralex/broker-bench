// Command bench runs the broker experiments in Docker Compose and summarizes them.
//
//	bench run [flags]        throughput: fill, drain and steady rates per profile
//	bench chaos [flags]      integrity: steady traffic, a killed node, every message checked
//	bench summarize [flags]  median tables of the latest experiments
//	bench report [flags]     write results/report.html with charts and optionally serve it
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var err error
	switch os.Args[1] {
	case "run":
		err = runCommand(ctx, os.Args[2:])
	case "chaos":
		err = chaosCommand(ctx, os.Args[2:])
	case "summarize":
		err = summarizeCommand(os.Args[2:])
	case "report":
		err = reportCommand(ctx, os.Args[2:])
	default:
		usage()
	}
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench run|chaos|summarize|report [flags]; use -h after a command for its flags")
	os.Exit(2)
}
