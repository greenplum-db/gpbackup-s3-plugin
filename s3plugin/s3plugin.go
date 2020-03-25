package s3plugin

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
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
		msg := fmt.Sprintf("Uploaded %d bytes for %s in %v", totalBytes,
			filepath.Base(fileName), elapsed.Round(time.Millisecond))
		gplog.Verbose(msg)
		fmt.Println(msg)
	}
	return err
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

func BackupDirectory(c *cli.Context) error {
	gplog.InitializeLogging("gpbackup", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dirName := c.Args().Get(1)
	fmt.Printf("dirKey = %s\n", dirName)
	fileList := make([]string, 0)
	_ = filepath.Walk(dirName, func(path string, f os.FileInfo, err error) error {
		isDir, _ := isDirectoryGetSize(path)
		if !isDir {
			fileList = append(fileList, path)
		}
		return nil
	})
	for _, fileName := range fileList {
		file, err := os.Open(fileName)
		if err != nil {
			return err
		}
		totalBytes, elapsed, err := uploadFile(sess, config.Options["bucket"], fileName, file)
		if err == nil {
			msg := fmt.Sprintf("Uploaded %d bytes for %s in %v", totalBytes,
				filepath.Base(fileName), elapsed.Round(time.Millisecond))
			gplog.Verbose(msg)
			fmt.Println(msg)
		}
		_ = file.Close()
	}
	return err
}

func BackupDirectoryParallel(c *cli.Context) error {
	gplog.InitializeLogging("gpbackup", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dirName := c.Args().Get(1)
	bucket := config.Options["bucket"]
	gplog.Verbose("Backup Directory '%s' to S3", dirName)
	gplog.Verbose("S3 Location = s3://%s/%s", bucket, dirName)

	// Create a list of files to be backed up
	fileList := make([]string, 0)
	_ = filepath.Walk(dirName, func(path string, f os.FileInfo, err error) error {
		isDir, _ := isDirectoryGetSize(path)
		if !isDir {
			fileList = append(fileList, path)
		}
		return nil
	})

	// Process the files in parallel
	var wg sync.WaitGroup
	var finalErr error
	for _, fileName := range fileList {
		wg.Add(1)
		go func(fileKey string) {
			defer wg.Done()
			file, err := os.Open(fileKey)
			if err != nil {
				finalErr = err
				return
			}
			totalBytes, elapsed, err := uploadFile(sess, bucket, fileKey, file)
			if err == nil {
				msg := fmt.Sprintf("Uploaded %d bytes for %s in %v", totalBytes,
					filepath.Base(fileName), elapsed.Round(time.Millisecond))
				gplog.Verbose(msg)
				fmt.Println(msg)
			} else {
				finalErr = err
			}
			_ = file.Close()
		}(fileName)
	}
	wg.Wait()
	return finalErr
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
		msg := fmt.Sprintf("Downloaded %d bytes for %s in %v", totalBytes,
			filepath.Base(fileKey), elapsed.Round(time.Millisecond))
		gplog.Verbose(msg)
		fmt.Println(msg)
	} else {
		_ = os.Remove(fileName)
	}
	return err
}

func RestoreDirectory(c *cli.Context) error {
	gplog.InitializeLogging("gprestore", "")
	start := time.Now()
	total := int64(0)
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	defer func() {
		fmt.Printf("Downloaded %d bytes in %v\n", total,
			time.Since(start).Round(time.Millisecond))
	}()
	dirName := c.Args().Get(1)
	bucket := config.Options["bucket"]
	gplog.Verbose("Restore Directory '%s' from S3", dirName)
	gplog.Verbose("S3 Location = s3://%s/%s", bucket, dirName)
	_ = os.MkdirAll(dirName, 0775)
	fmt.Printf("dirKey = %s\n", dirName)
	client := s3.New(sess)
	params := &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &dirName}
	bucketObjectsList, _ := client.ListObjectsV2(params)

	for _, key := range bucketObjectsList.Contents {
		var filename string
		if strings.HasSuffix(*key.Key, "/") {
			// Got a directory
			continue
		}
		if strings.Contains(*key.Key, "/") {
			// split
			s3FileFullPathList := strings.Split(*key.Key, "/")
			filename = s3FileFullPathList[len(s3FileFullPathList)-1]
		}
		filePath := dirName + "/" + filename
		file, err := os.Create(filePath)
		if err != nil {
			return err
		}
		totalBytes, elapsed, err := downloadFile(sess, config.Options["bucket"], *key.Key, file)
		if err == nil {
			total += totalBytes
			msg := fmt.Sprintf("Downloaded %d bytes for %s in %v", totalBytes,
				filepath.Base(*key.Key), elapsed.Round(time.Millisecond))
			gplog.Verbose(msg)
			fmt.Println(msg)
		} else {
			_ = os.Remove(filename)
		}
		_ = file.Close()
	}
	return err
}

func RestoreDirectoryParallel(c *cli.Context) error {
	gplog.InitializeLogging("gprestore", "")
	start := time.Now()
	total := int64(0)
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	defer func() {
		fmt.Printf("Downloaded %d bytes in %v\n", total,
			time.Since(start).Round(time.Millisecond))
	}()
	dirName := c.Args().Get(1)
	bucket := config.Options["bucket"]
	gplog.Verbose("Restore Directory '%s' from S3", dirName)
	gplog.Verbose("S3 Location = s3://%s/%s", bucket, dirName)
	_ = os.MkdirAll(dirName, 0775)
	client := s3.New(sess)
	params := &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &dirName}
	bucketObjectsList, _ := client.ListObjectsV2(params)

	// Create a list of files to be restored
	fileList := make([]string, 0)
	for _, key := range bucketObjectsList.Contents {
		gplog.Verbose("File '%s' = %d bytes", filepath.Base(*key.Key), *key.Size)
		if strings.HasSuffix(*key.Key, "/") {
			// Got a directory
			continue
		}
		fileList = append(fileList, *key.Key)
	}

	// Process the files in parallel
	var wg sync.WaitGroup
	var finalErr error
	for _, fileKey := range fileList {
		wg.Add(1)
		go func(fileKey string) {
			defer wg.Done()
			fileName := fileKey
			if strings.Contains(fileKey, "/") {
				fileName = filepath.Base(fileKey)
			}
			// construct local file name
			filePath := dirName + "/" + fileName
			file, err := os.Create(filePath)
			if err != nil {
				finalErr = err
				return
			}
			totalBytes, elapsed, err := downloadFile(sess, bucket, fileKey, file)
			if err == nil {
				total += totalBytes
				msg := fmt.Sprintf("Downloaded %d bytes for %s in %v", totalBytes,
					filepath.Base(fileKey), elapsed.Round(time.Millisecond))
				gplog.Verbose(msg)
				fmt.Println(msg)
			} else {
				finalErr = err
				_ = os.Remove(filePath)
			}
			_ = file.Close()
		}(fileKey)
	}
	wg.Wait()
	return finalErr
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

// 500 MB per part, supporting a file size up to 5TB
const UploadChunkSize = int64(units.Mebibyte) * 500

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

const DownloadChunkSize = int64(units.Mebibyte) * 100
/*
 * Performs ranged requests for the file while exploiting parallelism between the copy and download tasks
 */
func downloadFile(sess *session.Session, bucket string, fileKey string,
	file *os.File) (int64, time.Duration, error) {

	var finalErr error
	start := time.Now()
	downloader := s3manager.NewDownloader(sess)

	totalBytes, err := getFileSize(downloader.S3, bucket, fileKey)
	if err != nil {
		return 0, -1, err
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
			chunkStart := time.Now()
			numBytes, err := io.Copy(file, bytes.NewReader(downloadBuffers[currChunk].Bytes()))
			if err != nil {
				finalErr = err
			}
			gplog.Verbose("Copied %d bytes (chunk %d) for %s in %v",
				numBytes, currChunk, filepath.Base(fileKey),
				time.Since(chunkStart).Round(time.Millisecond))
			waitGroup.Done()
		}
	}()

	startByte := int64(0)
	endByte := DownloadChunkSize - 1
	for currentChunkNo := 0; currentChunkNo < noOfChunks; currentChunkNo++ {
		if endByte > totalBytes {
			endByte = totalBytes
		}
		chunkStart := time.Now()
		_, err := downloader.Download(downloadBuffers[currentChunkNo], &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fileKey),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startByte, endByte)),
		})
		if err != nil {
			finalErr = err
			break
		}
		gplog.Verbose("Downloaded %d bytes (chunk %d) for %s in %v",
			endByte - startByte + 1, currentChunkNo, filepath.Base(fileKey),
			time.Since(chunkStart).Round(time.Millisecond))
		waitGroup.Add(1)
		copyChannel <- currentChunkNo

		startByte += DownloadChunkSize
		endByte += DownloadChunkSize
	}
	close(copyChannel)
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