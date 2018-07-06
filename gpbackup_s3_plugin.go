// +build gpbackup_s3_plugin

package main

import (
	"log"
	"os"
	"fmt"

	"github.com/greenplum-db/gpbackup-s3-plugin/s3plugin"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version",
		Usage: "print version of gpbackup_s3_plugin",
	}
	app.Version = s3plugin.Version
	app.Usage = "S3 plugin for gpbackup and gprestore"

	app.Commands = []cli.Command{
		{
			Name:   "setup_plugin_for_backup",
			Action: s3plugin.SetupPluginForBackup,
			Before: buildBeforeFunc(3),
		},
		{
			Name:   "setup_plugin_for_restore",
			Action: s3plugin.SetupPluginForRestore,
			Before: buildBeforeFunc(3),
		},
		{
			Name:   "cleanup_plugin_for_backup",
			Action: s3plugin.CleanupPlugin,
			Before: buildBeforeFunc(3),
		},
		{
			Name:   "cleanup_plugin_for_restore",
			Action: s3plugin.CleanupPlugin,
			Before: buildBeforeFunc(3),
		},
		{
			Name:   "backup_file",
			Action: s3plugin.BackupFile,
			Before: buildBeforeFunc(2),
		},
		{
			Name:   "restore_file",
			Action: s3plugin.RestoreFile,
			Before: buildBeforeFunc(2),
		},
		{
			Name:   "backup_data",
			Action: s3plugin.BackupData,
			Before: buildBeforeFunc(2),
		},
		{
			Name:   "restore_data",
			Action: s3plugin.RestoreData,
			Before: buildBeforeFunc(2),
		},
		{
			Name:   "plugin_api_version",
			Action: s3plugin.GetAPIVersion,
			Before: buildBeforeFunc(0),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func buildBeforeFunc(expectedNArg int)(beforeFunc cli.BeforeFunc) {
	 beforeFunc = func(context *cli.Context) error {
		if actualNArg := context.NArg(); actualNArg != expectedNArg {
			return fmt.Errorf("Invalid number of arguments to plugin command. " +
				"Expected %d arguments. Got %d arguments", expectedNArg, actualNArg)
		}
		return nil
	}
	return beforeFunc
}
