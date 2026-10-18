package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/REPLACE-ME/agent-site/internal/config"
	"github.com/REPLACE-ME/agent-site/internal/httpx"
	"github.com/REPLACE-ME/agent-site/internal/store"
	"github.com/REPLACE-ME/agent-site/migrations"
)

// Main

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(config.ModeServer)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	db, err := store.Open(ctx, cfg.TursoURL, cfg.TursoToken)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := migrations.Up(ctx, db.DB); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	handler := httpx.Chain(
		newRouter(cfg, db),
		httpx.RequestID,
		httpx.RequestLogger,
		httpx.SecurityHeaders,
	)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		return fmt.Errorf("listen: %w", err)
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shutCancel()
	return srv.Shutdown(shutCtx)
}

// Router

func newRouter(cfg *config.Config, db *store.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := db.DB.PingContext(ctx); err != nil {
			http.Error(w, "db unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	return mux
}