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

	tbl.SetTitle("Channels -> Streams")

	tbl.AppendHeader(table.Row{
		"Name",
		"First Sequence",
		"Last Sequence",
	})

	for _, c := range result.Channels {
		tbl.AppendRow(table.Row{
			fmt.Sprintf("%s -> %s", c.Channel, c.Stream),
			fmt.Sprintf("%d -> %d", c.ChannelFirstSeq, c.StreamFirstSeq),
			fmt.Sprintf("%d -> %d", c.ChannelLastSeq, c.StreamLastSeq),
		})
	}

	tbl.SortBy([]table.SortBy{
		{Name: "Name", Mode: table.Asc},
	})

	tbl.Render()

	tbl = table.NewWriter()
	tbl.SetOutputMirror(os.Stdout)
	tbl.SetStyle(table.StyleRounded)

	tbl.SetTitle("Subscriptions -> Consumers")

	tbl.AppendHeader(table.Row{
		"Client",
		"Channel -> Stream",
		"Name",
		"Queue Name",
		"Converted to Pull?",
		"Next Sequence",
	})

	for _, s := range result.Subscriptions {
		tbl.AppendRow(table.Row{
			s.Client,
			fmt.Sprintf("%s -> %s", s.Channel, s.Stream),
			fmt.Sprintf("%s -> %s", s.Subscription, s.Consumer),
			s.Queue,
			s.Pull,
			fmt.Sprintf("%d -> %d", s.ChannelNextSeq, s.StreamNextSeq),
		})
	}

	tbl.SortBy([]table.SortBy{
		{Name: "Client", Mode: table.Asc},
		{Name: "Name", Mode: table.Asc},
	})

	tbl.Render()

	return nil
}
