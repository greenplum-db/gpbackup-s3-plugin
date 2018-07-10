package s3plugin

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
	"path/filepath"
)

var Version string

type Scope string

const (
	Master      Scope = "master"
	SegmentHost Scope = "segment_host"
	Segment     Scope = "segment"
)

func SetupPluginForBackup(c *cli.Context) error {
	if scope := (Scope)(c.Args().Get(2)); scope == Master || scope == SegmentHost {
		config, sess, err := readConfigAndStartSession(c)
		if err != nil {
			return err
		}
		localBackupDir := c.Args().Get(1)
		_, timestamp := filepath.Split(localBackupDir)
		testFilePath := fmt.Sprintf("%s/gpbackup_%s_report", localBackupDir, timestamp)
		fileKey := GetS3Path(config.Options["folder"], testFilePath)
		reader := strings.NewReader("")
		if err = uploadFile(sess, config.Options["bucket"], fileKey, reader); err != nil {
			return err
		}
	}
	return nil
}

func SetupPluginForRestore(c *cli.Context) error {
	if scope := (Scope)(c.Args().Get(2)); scope == Master || scope == SegmentHost {
		_, err := readAndValidatePluginConfig(c.Args().Get(0))
		return err
	}
	return nil
}

func CleanupPlugin(c *cli.Context) error {
	return nil
}

func BackupFile(c *cli.Context) error {
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	filename := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], filename)
	reader, err := os.Open(filename)
	if err != nil {
		return err
	}
	if err = uploadFile(sess, config.Options["bucket"], fileKey, reader); err != nil {
		return err
	}
	return nil
}

func RestoreFile(c *cli.Context) error {
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	filename := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], filename)
	writer, err := os.Create(filename)
	if err != nil {
		return err
	}
	if err = downloadFile(sess, config.Options["bucket"], fileKey, writer); err != nil {
		return err
	}
	return nil
}

func BackupData(c *cli.Context) error {
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], dataFile)
	reader := bufio.NewReader(os.Stdin)
	if err = uploadFile(sess, config.Options["bucket"], fileKey, reader); err != nil {
		return err
	}
	return nil
}

func RestoreData(c *cli.Context) error {
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], dataFile)
	if err = downloadFile(sess, config.Options["bucket"], fileKey, os.Stdout); err != nil {
		return err
	}
	return nil
}

func GetAPIVersion(c *cli.Context) {
	fmt.Println("0.2.0")
}

/*
 * Helper Functions
 */
type PluginConfig struct {
	ExecutablePath string
	Options        map[string]string
}

func readAndValidatePluginConfig(configFile string) (*PluginConfig, error) {
	config := &PluginConfig{}
	contents, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(contents, config); err != nil {
		return nil, err
	}
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func ValidateConfig(config *PluginConfig) error {
	requiredKeys := []string{"aws_access_key_id", "aws_secret_access_key", "bucket", "folder"}
	for _, key := range requiredKeys {
		if config.Options[key] == "" {
			return fmt.Errorf("%s must exist in plugin configuration file", key)
		}
	}

	if config.Options["region"] == "" {
		if config.Options["endpoint"] == "" {
			return fmt.Errorf("region or endpoint must exist in plugin configuration file")
		}
		config.Options["region"] = "unused"
	}

	return nil
}

func readConfigAndStartSession(c *cli.Context) (*PluginConfig, *session.Session, error) {
	configPath := c.Args().Get(0)
	config, err := readAndValidatePluginConfig(configPath)
	if err != nil {
		return nil, nil, err
	}
	disableSSL := !ShouldEnableEncryption(config)
	creds := credentials.NewStaticCredentials(config.Options["aws_access_key_id"],
		config.Options["aws_secret_access_key"], "")
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(config.Options["region"]),
		Endpoint:         aws.String(config.Options["endpoint"]),
		Credentials:      creds,
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(disableSSL),
	})
	if err != nil {
		return nil, nil, err
	}
	return config, sess, nil
}

func ShouldEnableEncryption(config *PluginConfig) bool {
	if strings.EqualFold(config.Options["encryption"], "off") {
		return false
	}
	return true
}

func uploadFile(sess *session.Session, bucket string, fileKey string, fileReader io.Reader) error {
	uploader := s3manager.NewUploader(sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
		Body:   fileReader,
	})
	return err
}

func downloadFile(sess *session.Session, bucket string, fileKey string, fileWriter io.Writer) error {
	buff := &aws.WriteAtBuffer{}
	downloader := s3manager.NewDownloader(sess)
	_, err := downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
	})
	if err != nil {
		return err
	}
	_, err = io.Copy(fileWriter, bytes.NewReader(buff.Bytes()))
	return err
}

func GetS3Path(folder string, path string) string {
	pathArray := strings.Split(path, "/")
	return fmt.Sprintf("%s/%s", folder, strings.Join(pathArray[(len(pathArray) - 4):], "/"))
}
