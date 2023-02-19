package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/nats-io/stan2js"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("config file is required")
	}

	configFile := os.Args[1]
	if configFile == "" {
		return errors.New("config file is required")
	}

	// Read the config from a YAML-encoded file.
	config, err := stan2js.ReadConfig(configFile)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	// Validate before running the migration.
	if err := config.Validate(); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	// Run the migration.
	result, err := stan2js.Migrate(config)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	tbl := table.NewWriter()
	tbl.SetOutputMirror(os.Stdout)
	tbl.SetStyle(table.StyleRounded)

	tbl.AppendHeader(table.Row{
		"Channel",
		"Stream",
		"Old Sequence",
		"New Sequence",
	})

	for _, c := range result.Channels {
		tbl.AppendRow(table.Row{
			c.Channel,
			c.Stream,
			c.OldSeq,
			c.NewSeq,
		})
	}

	tbl.Render()

	tbl = table.NewWriter()
	tbl.SetOutputMirror(os.Stdout)
	tbl.SetStyle(table.StyleRounded)

	tbl.AppendHeader(table.Row{
		"Client",
		"Channel",
		"Stream",
		"Subscription",
		"Consumer",
		"Queue",
		"Pull",
		"Old Sequence",
		"New Sequence",
	})

	for _, s := range result.Subscriptions {
		tbl.AppendRow(table.Row{
			s.Client,
			s.Channel,
			s.Stream,
			s.Subscription,
			s.Consumer,
			s.Queue,
			s.Pull,
			s.OldSeq,
			s.NewSeq,
		})
	}

	tbl.Render()

	return nil
}
