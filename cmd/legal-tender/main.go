// Command legal-tender is the single Go entry point for pipeline and API work.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/vedantadhobley/legal-tender/internal/app/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(cli.RunContext(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
