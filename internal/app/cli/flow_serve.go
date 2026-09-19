package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/serving/flowapi"
)

func runServeCommitteeFlow(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("serve committee-flow-evidence", flag.ContinueOnError)
	flags.SetOutput(stderr)
	o := flowevidence.Options{}
	flags.StringVar(&o.StorageRoot, "storage-root", "", "read-only published storage root")
	flags.StringVar(&o.Bundle, "projection-bundle", "", "exact observation readiness manifest")
	flags.StringVar(&o.Cycle, "cycle", "", "expected FEC cycle")
	flags.StringVar(&o.Endpoint, "endpoint", "", "Arango endpoint without credentials")
	flags.StringVar(&o.Username, "username", os.Getenv("ARANGO_USER"), "configured read-only Arango username")
	password := flags.String("password-env", "ARANGO_PASSWORD", "environment variable containing password")
	listen := flags.String("listen", "127.0.0.1:8080", "HTTP listen address; no host-port publishing implied")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || o.StorageRoot == "" || o.Bundle == "" || !validPeriod(o.Cycle) || o.Endpoint == "" || o.Username == "" {
		fmt.Fprintln(stderr, "storage root, exact bundle, cycle, endpoint and username required")
		return 2
	}
	if _, _, err := net.SplitHostPort(*listen); err != nil {
		fmt.Fprintln(stderr, "invalid listen address")
		return 2
	}
	o.Password = os.Getenv(*password)
	fmt.Fprintln(stderr, "verifying the pinned graph and source backing before serving")
	reader, err := flowevidence.OpenReader(ctx, o)
	if err != nil {
		fmt.Fprintln(stderr, "evidence reader startup failed:", err)
		return 1
	}
	handler, err := flowapi.NewHandler(reader)
	if err != nil {
		fmt.Fprintln(stderr, "cannot initialize HTTP reader")
		return 1
	}
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(stderr, "cannot bind HTTP listener")
		return 1
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if server.Shutdown(shutdown) != nil {
				_ = server.Close()
			}
		case <-stopped:
		}
	}()
	if err := encodeJSON(stdout, struct {
		State      string            `json:"state"`
		Address    string            `json:"address"`
		Projection flowevidence.View `json:"projection"`
	}{"listening", l.Addr().String(), reader.View()}); err != nil {
		_ = l.Close()
		close(stopped)
		return 1
	}
	err = server.Serve(l)
	close(stopped)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(stderr, "HTTP evidence server stopped unexpectedly")
		return 1
	}
	return 0
}
