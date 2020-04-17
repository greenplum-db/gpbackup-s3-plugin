package s3plugin

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

var Version string

const apiVersion = "0.4.0"
const Mebibyte = 1024 * 1024
const Concurrency = 6

type Scope string

const (
	Master      Scope = "master"
	SegmentHost Scope = "segment_host"
	Segment     Scope = "segment"
)

const (
	Gpbackup  string = "Gpbackup"
	Gprestore string = "Gprestore"
)

type PluginConfig struct {
	ExecutablePath string
	Options        map[string]string
}

func CleanupPlugin(c *cli.Context) error {
	return nil
}

func GetAPIVersion(c *cli.Context) {
	fmt.Println(apiVersion)
}

/*
 * Helper Functions
 */

func readAndValidatePluginConfig(configFile string) (*PluginConfig, error) {
	config := &PluginConfig{}
	contents, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	if err = yaml.Unmarshal(contents, config); err != nil {
		return nil, err
	}
	if err = ValidateConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func ValidateConfig(config *PluginConfig) error {
	requiredKeys := []string{"bucket", "folder"}
	for _, key := range requiredKeys {
		if config.Options[key] == "" {
			return fmt.Errorf("%s must exist in plugin configuration file", key)
		}
	}

	if config.Options["aws_access_key_id"] == "" {
		if config.Options["aws_secret_access_key"] != "" {
			return fmt.Errorf("aws_access_key_id must exist in plugin configuration file if aws_secret_access_key does")
		}
	} else if config.Options["aws_secret_access_key"] == "" {
		return fmt.Errorf("aws_secret_access_key must exist in plugin configuration file if aws_access_key_id does")
	}

	if config.Options["region"] == "" {
		if config.Options["endpoint"] == "" {
			return fmt.Errorf("region or endpoint must exist in plugin configuration file")
		}
		config.Options["region"] = "unused"
	}

	return nil
}

func readConfigAndStartSession(c *cli.Context, operation string) (*PluginConfig, *session.Session, error) {
	configPath := c.Args().Get(0)
	config, err := readAndValidatePluginConfig(configPath)
	if err != nil {
		return nil, nil, err
	}
	disableSSL := !ShouldEnableEncryption(config)

	ShouldEnableDebug(config, operation)
	awsConfig := aws.NewConfig().
		WithRegion(config.Options["region"]).
		WithEndpoint(config.Options["endpoint"]).
		WithS3ForcePathStyle(true).
		WithDisableSSL(disableSSL)

	// Will use default credential chain if none provided
	if config.Options["aws_access_key_id"] != "" {
		awsConfig = awsConfig.WithCredentials(
			credentials.NewStaticCredentials(
				config.Options["aws_access_key_id"],
				config.Options["aws_secret_access_key"], ""))
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, nil, err
	}
	return config, sess, nil
}

func ShouldEnableDebug(config *PluginConfig, operation string) {
	gplog.InitializeLogging(operation, "")
	verbosity := gplog.LOGINFO
	if strings.EqualFold(config.Options["debug"], "on") {
		verbosity = gplog.LOGDEBUG
	}
	gplog.SetVerbosity(verbosity)
}

func GetDebug() int {
	return gplog.GetVerbosity()
}

func ShouldEnableEncryption(config *PluginConfig) bool {
	isOff := strings.EqualFold(config.Options["encryption"], "off")
	return !isOff
}

func isDirectoryGetSize(path string) (bool, int64) {
	fd, err := os.Stat(path)
	if err != nil {
		gplog.FatalOnError(err)
	}
	switch mode := fd.Mode(); {
	case mode.IsDir():
		return true, 0
	case mode.IsRegular():
		return false, fd.Size()
	}
	gplog.FatalOnError(errors.New(fmt.Sprintf("INVALID file %s", path)))
	return false, 0
}

func getFileSize(S3 s3iface.S3API, bucket string, fileKey string) (int64, error) {
	req, resp := S3.HeadObjectRequest(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
	})
	err := req.Send()

	if err != nil {
		return 0, err
	}
	return *resp.ContentLength, nil
}

func GetS3Path(folder string, path string) string {
	/*
			a typical path for an already-backed-up file will be stored in a
			parent directory of a segment, and beneath that, under a datestamp/timestamp/
		    hierarchy. We assume the incoming path is a long absolute one.
			For example from the test bench:
			  testdir_for_del="/tmp/testseg/backups/$current_date_for_del/$time_second_for_del"
			  testfile_for_del="$testdir_for_del/testfile_$time_second_for_del.txt"

			Therefore, the incoming path is relevant to S3 in only the last four segments,
			which indicate the file and its 2 date/timestamp parents, and the grandparent "backups"
	*/
	pathArray := strings.Split(path, "/")
	lastFour := strings.Join(pathArray[(len(pathArray)-4):], "/")
	return fmt.Sprintf("%s/%s", folder, lastFour)
}

func DeleteBackup(c *cli.Context) error {
	timestamp := c.Args().Get(1)
	if timestamp == "" {
		return errors.New("delete requires a <timestamp>")
	}

	if !IsValidTimestamp(timestamp) {
		msg := fmt.Sprintf("delete requires a <timestamp> with format "+
			"YYYYMMDDHHMMSS, but received: %s", timestamp)
		return fmt.Errorf(msg)
	}

	date := timestamp[0:8]
	// note that "backups" is a directory is a fact of how we save, choosing
	// to use the 3 parent directories of the source file. That becomes:
	// <s3folder>/backups/<date>/<timestamp>
	config, sess, err := readConfigAndStartSession(c, Gpbackup)
	if err != nil {
		return err
	}
	deletePath := filepath.Join(config.Options["folder"], "backups", date, timestamp)
	bucket := config.Options["bucket"]
	gplog.Debug("Delete location = s3://%s/%s", bucket, deletePath)

	service := s3.New(sess)
	iter := s3manager.NewDeleteListIterator(service, &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(deletePath),
	})

	batchClient := s3manager.NewBatchDeleteWithClient(service)
	return batchClient.Delete(aws.BackgroundContext(), iter)
}

func IsValidTimestamp(timestamp string) bool {
	timestampFormat := regexp.MustCompile(`^([0-9]{14})$`)
	return timestampFormat.MatchString(timestamp)
}
