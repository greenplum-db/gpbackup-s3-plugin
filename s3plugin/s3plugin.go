package s3plugin

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/inhies/go-bytesize"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

var Version string

const apiVersion = "0.4.0"
const Mebibyte = 1024 * 1024
const DefaultConcurrency = 6
const DefaultUploadChunkSize = int64(Mebibyte) * 500   // default 500MB
const DefaultDownloadChunkSize = int64(Mebibyte) * 500 // default 500MB

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
	ExecutablePath string        `yaml:"executablepath"`
	Options        PluginOptions `yaml:"options"`
}

type PluginOptions struct {
	AwsAccessKeyId               string `yaml:"aws_access_key_id"`
	AwsSecretAccessKey           string `yaml:"aws_secret_access_key"`
	BackupMaxConcurrentRequests  string `yaml:"backup_max_concurrent_requests"`
	BackupMultipartChunksize     string `yaml:"backup_multipart_chunksize"`
	Bucket                       string `yaml:"bucket"`
	Encryption                   string `yaml:"encryption"`
	Endpoint                     string `yaml:"endpoint"`
	Folder                       string `yaml:"folder"`
	HttpProxy                    string `yaml:"http_proxy"`
	Region                       string `yaml:"region"`
	RestoreMaxConcurrentRequests string `yaml:"restore_max_concurrent_requests"`
	RestoreMultipartChunksize    string `yaml:"restore_multipart_chunksize"`

	UploadChunkSize     int64
	UploadConcurrency   int
	DownloadChunkSize   int64
	DownloadConcurrency int
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
	if err = yaml.UnmarshalStrict(contents, config); err != nil {
		return nil, fmt.Errorf("Yaml failures encountered reading config file %s. Error: %s", configFile, err.Error())
	}
	if err = InitializeAndValidateConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func InitializeAndValidateConfig(config *PluginConfig) error {
	var err error
	var errTxt string
	opt := &config.Options

	// Initialize defaults
	if opt.Region == "" {
		opt.Region = "unused"
	}
	if opt.Encryption == "" {
		opt.Encryption = "yes"
	}
	opt.UploadChunkSize = DefaultUploadChunkSize
	opt.UploadConcurrency = DefaultConcurrency
	opt.DownloadChunkSize = DefaultDownloadChunkSize
	opt.DownloadConcurrency = DefaultConcurrency

	// Validate configurations and overwrite defaults
	if config.ExecutablePath == "" {
		errTxt += fmt.Sprintf("executable_path must exist and cannot be empty in plugin configuration file\n")
	}
	if opt.Bucket == "" {
		errTxt += fmt.Sprintf("bucket must exist and cannot be empty in plugin configuration file\n")
	}
	if opt.Folder == "" {
		errTxt += fmt.Sprintf("folder must exist and cannot be empty in plugin configuration file\n")
	}
	if opt.AwsAccessKeyId == "" {
		if opt.AwsSecretAccessKey != "" {
			errTxt += fmt.Sprintf("aws_access_key_id must exist in plugin configuration file if aws_secret_access_key does\n")
		}
	} else if opt.AwsSecretAccessKey == "" {
		errTxt += fmt.Sprintf("aws_secret_access_key must exist in plugin configuration file if aws_access_key_id does\n")
	}
	if opt.Region == "unused" && opt.Endpoint == "" {
		errTxt += fmt.Sprintf("region or endpoint must exist in plugin configuration file\n")
	}
	if opt.Encryption != "yes" && opt.Encryption != "no" {
		errTxt += fmt.Sprintf("Invalid encryption configuration. Valid choices are yes or no.\n")
	}
	if opt.BackupMultipartChunksize != "" {
		chunkSize, err := bytesize.Parse(opt.BackupMultipartChunksize)
		if err != nil {
			errTxt += fmt.Sprintf("Invalid backup_multipart_chunksize. Err: %s\n", err)
		}
		// Chunk size is being converted from uint64 to int64. This is safe as
		// long as chunksize smaller than math.MaxInt64 bytes (~9223 Petabytes)
		opt.UploadChunkSize = int64(chunkSize)
	}
	if opt.BackupMaxConcurrentRequests != "" {
		opt.UploadConcurrency, err = strconv.Atoi(opt.BackupMaxConcurrentRequests)
		if err != nil {
			errTxt += fmt.Sprintf("Invalid backup_max_concurrent_requests. Err: %s\n", err)
		}
	}
	if opt.RestoreMultipartChunksize != "" {
		chunkSize, err := bytesize.Parse(opt.RestoreMultipartChunksize)
		if err != nil {
			errTxt += fmt.Sprintf("Invalid restore_multipart_chunksize. Err: %s\n", err)
		}
		// Chunk size is being converted from uint64 to int64. This is safe as
		// long as chunksize smaller than math.MaxInt64 bytes (~9223 Petabytes)
		opt.DownloadChunkSize = int64(chunkSize)
	}
	if opt.RestoreMaxConcurrentRequests != "" {
		opt.DownloadConcurrency, err = strconv.Atoi(opt.RestoreMaxConcurrentRequests)
		if err != nil {
			errTxt += fmt.Sprintf("Invalid restore_max_concurrent_requests. Err: %s\n", err)
		}
	}

	if errTxt != "" {
		return errors.New(errTxt)
	}
	return nil
}

func readConfigAndStartSession(c *cli.Context, operation string) (*PluginConfig, *session.Session, error) {
	configPath := c.Args().Get(0)
	config, err := readAndValidatePluginConfig(configPath)
	if err != nil {
		return nil, nil, err
	}

	disableSSL := !ShouldEnableEncryption(config.Options.Encryption)

	awsConfig := aws.NewConfig().
		WithRegion(config.Options.Region).
		WithEndpoint(config.Options.Endpoint).
		WithS3ForcePathStyle(true).
		WithDisableSSL(disableSSL).
		WithUseDualStack(true)

	// Will use default credential chain if none provided
	if config.Options.AwsAccessKeyId != "" {
		awsConfig = awsConfig.WithCredentials(
			credentials.NewStaticCredentials(
				config.Options.AwsAccessKeyId,
				config.Options.AwsSecretAccessKey, ""))
	}

	if config.Options.HttpProxy != "" {
		httpclient := &http.Client{
			Transport: &http.Transport{
				Proxy: func(*http.Request) (*url.URL, error) {
					return url.Parse(config.Options.HttpProxy)
				},
			},
		}
		awsConfig.WithHTTPClient(httpclient)
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, nil, err
	}
	return config, sess, nil
}

func ShouldEnableEncryption(encryption string) bool {
	isOff := strings.EqualFold(encryption, "off")
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
	deletePath := filepath.Join(config.Options.Folder, "backups", date, timestamp)
	bucket := config.Options.Bucket
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
