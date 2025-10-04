package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"

	"modbus_processor/internal/config"
	"modbus_processor/internal/logging"
	"modbus_processor/internal/service"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load configuration")
	}

	logger, cleanup, err := logging.Setup(cfg.Logging)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to setup logger")
	}
	defer cleanup()
	log.Logger = logger

	srv, err := service.New(cfg, logger, nil)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create service")
	}
	defer srv.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := srv.Run(ctx); err != nil {
		logger.Fatal().Err(err).Msg("service stopped with error")
	}
}
