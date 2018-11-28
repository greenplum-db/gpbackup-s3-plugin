package s3plugin

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"bytes"
	"math"
	"sync"

	"github.com/alecthomas/units"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

var Version string

const API_VERSION = "0.3.0"

type Scope string

const (
	Master      Scope = "master"
	SegmentHost Scope = "segment_host"
	Segment     Scope = "segment"
)

const DownloadChunkSize = int64(units.Mebibyte) * 100

func SetupPluginForBackup(c *cli.Context) error {
	args := c.Args()
	if scope := (Scope)(args.Get(2)); scope == Master || scope == SegmentHost {
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
	fmt.Println(API_VERSION)
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

func readConfigAndStartSession(c *cli.Context) (*PluginConfig, *session.Session, error) {
	configPath := c.Args().Get(0)
	config, err := readAndValidatePluginConfig(configPath)
	if err != nil {
		return nil, nil, err
	}
	disableSSL := !ShouldEnableEncryption(config)

	awsConfig := aws.NewConfig().WithRegion(config.Options["region"]).WithEndpoint(config.Options["endpoint"]).WithS3ForcePathStyle(true).WithDisableSSL(disableSSL)

	// Will use default credential chain if none provided
	if config.Options["aws_access_key_id"] != "" {
		awsConfig = awsConfig.WithCredentials(credentials.NewStaticCredentials(config.Options["aws_access_key_id"],
			config.Options["aws_secret_access_key"], ""))
	}

	sess, err := session.NewSession(awsConfig)

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
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		// 500 MB per part, supporting a file size up to 5TB
		u.PartSize = 500 * 1024 * 1024
	})
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
		Body:   fileReader,
	})
	return err
}

/*
 * Performs ranged requests for the file while exploiting parallelism between the copy and download tasks
 */
func downloadFile(sess *session.Session, bucket string, fileKey string, fileWriter io.Writer) error {
	var finalErr error
	downloader := s3manager.NewDownloader(sess)

	totalBytes, err := getFileSize(downloader, bucket, fileKey)
	if err != nil {
		return err
	}
	noOfChunks := int(math.Ceil(float64(totalBytes) / float64(DownloadChunkSize)))
	downloadBuffers := make([]*aws.WriteAtBuffer, noOfChunks)
	for i := 0; i < noOfChunks; i++ {
		downloadBuffers[i] = &aws.WriteAtBuffer{GrowthCoeff: 2}
	}
	copyChannel := make(chan int)

	waitGroup := sync.WaitGroup{}

	go func() {
		for currChunk := range copyChannel {
			_, err := io.Copy(fileWriter, bytes.NewReader(downloadBuffers[currChunk].Bytes()))
			if err != nil {
				finalErr = err
			}
			waitGroup.Done()
		}
	}()

	startByte := int64(0)
	endByte := int64(DownloadChunkSize - 1)
	for currentChunkNo := 0; currentChunkNo < noOfChunks; currentChunkNo++ {
		if endByte > totalBytes {
			endByte = totalBytes
		}
		_, err := downloader.Download(downloadBuffers[currentChunkNo], &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fileKey),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startByte, endByte)),
		})
		if err != nil {
			finalErr = err
			break
		}

		waitGroup.Add(1)

		copyChannel <- currentChunkNo

		startByte += DownloadChunkSize
		endByte += DownloadChunkSize
	}
	close(copyChannel)

	waitGroup.Wait()

	return finalErr
}

func getFileSize(downloader *s3manager.Downloader, bucket string, fileKey string) (int64, error) {
	req, resp := downloader.S3.HeadObjectRequest(&s3.HeadObjectInput{
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
	pathArray := strings.Split(path, "/")
	return fmt.Sprintf("%s/%s", folder, strings.Join(pathArray[(len(pathArray)-4):], "/"))
}
