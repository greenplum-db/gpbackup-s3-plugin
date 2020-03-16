package s3plugin

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/units"
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

type Scope string

const (
	Master      Scope = "master"
	SegmentHost Scope = "segment_host"
	Segment     Scope = "segment"
)

type PluginConfig struct {
	ExecutablePath string
	Options        map[string]string
}

func SetupPluginForBackup(c *cli.Context) error {
	scope := (Scope)(c.Args().Get(2))
	if scope != Master && scope != SegmentHost {
		return nil
	}

	gplog.InitializeLogging("gpbackup", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	localBackupDir := c.Args().Get(1)
	_, timestamp := filepath.Split(localBackupDir)
	testFilePath := fmt.Sprintf("%s/gpbackup_%s_report", localBackupDir, timestamp)
	fileKey := GetS3Path(config.Options["folder"], testFilePath)
	file, err := os.Create(testFilePath) // dummy empty reader for probe
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}
	_, _, err = uploadFile(sess, config.Options["bucket"], fileKey, file)
	return err
}

func SetupPluginForRestore(c *cli.Context) error {
	scope := (Scope)(c.Args().Get(2))
	if scope != Master && scope != SegmentHost {
		return nil
	}
	gplog.InitializeLogging("gprestore", "")
	_, err := readAndValidatePluginConfig(c.Args().Get(0))
	return err
}

func CleanupPlugin(c *cli.Context) error {
	return nil
}

func BackupFile(c *cli.Context) error {
	gplog.InitializeLogging("gpbackup", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	fileName := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], fileName)
	file, err := os.Open(fileName)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}
	totalBytes, elapsed, err := uploadFile(sess, config.Options["bucket"], fileKey, file)
	if err == nil {
		gplog.Verbose("Uploaded %d bytes for %s in %v", totalBytes,
			filepath.Base(fileKey), elapsed.Round(time.Millisecond))
	} else {
		gplog.FatalOnError(err)
	}
	return err
}

func RestoreFile(c *cli.Context) error {
	gplog.InitializeLogging("gprestore", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	fileName := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], fileName)
	file, err := os.Create(fileName)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}
	totalBytes, elapsed, err := downloadFile(sess, config.Options["bucket"], fileKey, file)
	if err == nil {
		gplog.Verbose("Downloaded %d bytes for file %s in %v", totalBytes,
			filepath.Base(fileKey), elapsed.Round(time.Millisecond))
	} else {
		gplog.FatalOnError(err)
		_ = os.Remove(fileName)
	}
	return err
}

func BackupData(c *cli.Context) error {
	gplog.InitializeLogging("gpbackup", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], dataFile)
	totalBytes, elapsed, err := uploadFile(sess, config.Options["bucket"], fileKey, os.Stdin)
	if err == nil {
		gplog.Verbose("Uploaded %d bytes for file %s in %v", totalBytes,
			filepath.Base(fileKey), elapsed.Round(time.Millisecond))
	} else {
		gplog.FatalOnError(err)
	}
	return err
}

func RestoreData(c *cli.Context) error {
	gplog.InitializeLogging("gprestore", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], dataFile)
	totalBytes, elapsed, err := downloadFile(sess, config.Options["bucket"], fileKey, os.Stdout)
	if err == nil {
		gplog.Verbose("Downloaded %d bytes for file %s in %v", totalBytes,
			filepath.Base(fileKey), elapsed.Round(time.Millisecond))
	} else {
		gplog.FatalOnError(err)
	}
	return err
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

func readConfigAndStartSession(c *cli.Context) (*PluginConfig, *session.Session, error) {
	configPath := c.Args().Get(0)
	config, err := readAndValidatePluginConfig(configPath)
	if err != nil {
		return nil, nil, err
	}
	disableSSL := !ShouldEnableEncryption(config)

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

func ShouldEnableEncryption(config *PluginConfig) bool {
	isOff := strings.EqualFold(config.Options["encryption"], "off")
	return !isOff
}

/*
     8 MB starting chunk where each subsequent chunk increments by 1MB
     AWS only allows upto 10000 chunks with Max file size of 5TB
     The limit would be reached at chunk number 3155 in our case
     Chunk sizes = 8, 9, 10, ..., 3155
 */
const DownloadChunkSize = int64(units.Mebibyte) * 8
const DownloadChunkIncrement = int64(units.Mebibyte) * 1
const UploadChunkSize = int64(units.Mebibyte) * 500
const Concurrency = 6

func uploadFile(sess *session.Session, bucket string, fileKey string,
	file *os.File) (int64, time.Duration, error) {

	start := time.Now()
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = UploadChunkSize
	})
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
		Body:   bufio.NewReader(file),
	})
	if err != nil {
		return 0, -1, err
	}
	totalBytes, err := getFileSize(uploader.S3, bucket, fileKey)
    return totalBytes, time.Since(start), err
}

type chunk struct {
	chunkIndex int
	startByte  int64
	endByte    int64
}

func calculateNumChunks(size int64) int {
	currentChunkSize := DownloadChunkSize
	numChunks := 1
	for total := size - currentChunkSize; total > 0; total -= currentChunkSize {
		currentChunkSize += DownloadChunkIncrement
		numChunks++
	}
	return numChunks
}

/*
 * Performs ranged requests for the file while exploiting parallelism between the copy and download tasks
 */
func downloadFile(sess *session.Session, bucket string, fileKey string,
	file *os.File) (int64, time.Duration, error) {

	start := time.Now()
	downloader := s3manager.NewDownloader(sess)

	totalBytes, err := getFileSize(downloader.S3, bucket, fileKey)
	if err != nil {
		return 0, -1, err
	}
	gplog.Verbose("File %s size = %d bytes", filepath.Base(fileKey), totalBytes)
	if totalBytes <= DownloadChunkSize {
		_, err = downloader.Download(
			file,
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(fileKey),
			})
	} else {
		return downloadFileInParallel(downloader, totalBytes, bucket, fileKey, file)
	}
	return totalBytes, time.Since(start), err
}

/*
 * Performs ranged requests for the file while exploiting parallelism between the copy and download tasks
 */
func downloadFileInParallel(downloader *s3manager.Downloader, totalBytes int64,
	bucket string, fileKey string, file *os.File) (int64, time.Duration, error) {

	var finalErr error
	start := time.Now()
	waitGroup := sync.WaitGroup{}
	numberOfChunks := calculateNumChunks(totalBytes)
	downloadBuffers := make([]*aws.WriteAtBuffer, numberOfChunks)
	copyChannel := make([]chan int, numberOfChunks)
	jobs := make(chan chunk, numberOfChunks)
	for i := 0; i < numberOfChunks; i++ {
		copyChannel[i] = make(chan int)
	}

	startByte := int64(0)
	endByte := int64(-1)
	done := false
	// Create jobs based on the number of chunks to be downloaded
	for chunkIndex := 0; chunkIndex < numberOfChunks && !done; chunkIndex++ {
		startByte = endByte + 1
		endByte += DownloadChunkSize + int64(chunkIndex) * DownloadChunkIncrement
		if endByte >= totalBytes {
			endByte = totalBytes - 1
			done = true
		}
		downloadBuffers[chunkIndex] = &aws.WriteAtBuffer{GrowthCoeff: 2}
		jobs <- chunk{
			chunkIndex,
			startByte,
			endByte,
		}
		waitGroup.Add(1)
	}

	// Create a pool of download workers (based on concurrency)
	numberOfWorkers := Concurrency
	if numberOfChunks < Concurrency {
		numberOfWorkers = numberOfChunks
	}
	for i := 0; i < numberOfWorkers; i++ {
		go func(id int) {
			for j := range jobs {
				chunkStart := time.Now()
				byteRange := fmt.Sprintf("bytes=%d-%d", j.startByte, j.endByte)
				chunkBytes, err := downloader.Download(
					downloadBuffers[j.chunkIndex],
					&s3.GetObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(fileKey),
						Range:  aws.String(byteRange),
					})
				if err != nil {
					finalErr = err
				}
				gplog.Verbose("Worker %d Downloaded %d bytes (chunk %d) for %s in %v",
					id, chunkBytes, j.chunkIndex, filepath.Base(fileKey),
					time.Since(chunkStart).Round(time.Millisecond))
				copyChannel[j.chunkIndex] <- j.chunkIndex
				time.Sleep(time.Millisecond * 10)
			}
		}(i)
	}

	// Copy data from download buffers into the output stream sequentially
	go func() {
		for i := range copyChannel {
			currentChunk := <- copyChannel[i]
			chunkStart := time.Now()
			numBytes, err := file.Write(downloadBuffers[currentChunk].Bytes())
			if err != nil {
				finalErr = err
			}
			gplog.Verbose("Copied %d bytes (chunk %d) for %s in %v",
				numBytes, currentChunk, filepath.Base(fileKey),
				time.Since(chunkStart).Round(time.Millisecond))
			waitGroup.Done()
			close(copyChannel[i])
		}
	}()

	waitGroup.Wait()

	return totalBytes, time.Since(start), finalErr
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

func Delete(c *cli.Context) error {
	timestamp := c.Args().Get(1)
	if timestamp == "" {
		return errors.New("delete requires a <timestamp>")
	}

	if !IsValidTimestamp(timestamp) {
		return fmt.Errorf("delete requires a <timestamp> with format " +
			"YYYYMMDDHHMMSS, but received: %s", timestamp)
	}

	date := timestamp[0:8]
	// note that "backups" is a directory is a fact of how we save, choosing
	// to use the 3 parent directories of the source file. That becomes:
	// <s3folder>/backups/<date>/<timestamp>
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	deletePath := filepath.Join(config.Options["folder"], "backups", date, timestamp)
	bucket := config.Options["bucket"]

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