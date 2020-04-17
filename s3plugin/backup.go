package s3plugin

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/urfave/cli"
)

func SetupPluginForBackup(c *cli.Context) error {
	scope := (Scope)(c.Args().Get(2))
	if scope != Master && scope != SegmentHost {
		return nil
	}
	config, sess, err := readConfigAndStartSession(c, Gpbackup)
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

func BackupFile(c *cli.Context) error {
	config, sess, err := readConfigAndStartSession(c, Gpbackup)
	if err != nil {
		return err
	}
	fileName := c.Args().Get(1)
	bucket := config.Options["bucket"]
	fileKey := GetS3Path(config.Options["folder"], fileName)
	file, err := os.Open(fileName)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}
	bytes, elapsed, err := uploadFile(sess, bucket, fileKey, file)
	if err == nil {
		msg := fmt.Sprintf("Uploaded %d bytes for %s in %v", bytes,
			filepath.Base(fileKey), elapsed.Round(time.Millisecond))
		gplog.Verbose(msg)
		fmt.Println(msg)
	} else {
		gplog.FatalOnError(err)
	}
	return err
}

func BackupDirectory(c *cli.Context) error {
	start := time.Now()
	totalBytes := int64(0)
	config, sess, err := readConfigAndStartSession(c, Gpbackup)
	if err != nil {
		return err
	}
	dirName := c.Args().Get(1)
	bucket := config.Options["bucket"]
	gplog.Verbose("Restore Directory '%s' from S3", dirName)
	gplog.Verbose("S3 Location = s3://%s/%s", bucket, dirName)
	fmt.Printf("dirKey = %s\n", dirName)

	// Populate a list of files to be backed up
	fileList := make([]string, 0)
	_ = filepath.Walk(dirName, func(path string, f os.FileInfo, err error) error {
		isDir, _ := isDirectoryGetSize(path)
		if !isDir {
			fileList = append(fileList, path)
		}
		return nil
	})

	// Process the files sequentially
	for _, fileName := range fileList {
		file, err := os.Open(fileName)
		if err != nil {
			return err
		}
		bytes, elapsed, err := uploadFile(sess, bucket, fileName, file)
		if err == nil {
			totalBytes += bytes
			msg := fmt.Sprintf("Uploaded %d bytes for %s in %v", bytes,
				filepath.Base(fileName), elapsed.Round(time.Millisecond))
			gplog.Verbose(msg)
			fmt.Println(msg)
		} else {
			gplog.FatalOnError(err)
		}
		_ = file.Close()
	}

	fmt.Printf("Uploaded %d files (%d bytes) in %v\n",
		len(fileList), totalBytes, time.Since(start).Round(time.Millisecond))
	return err
}

func BackupDirectoryParallel(c *cli.Context) error {
	start := time.Now()
	totalBytes := int64(0)
	parallel := 5
	config, sess, err := readConfigAndStartSession(c, Gpbackup)
	if err != nil {
		return err
	}
	dirName := c.Args().Get(1)
	if len(c.Args()) == 3 {
		parallel, _ = strconv.Atoi(c.Args().Get(2))
	}
	bucket := config.Options["bucket"]
	gplog.Verbose("Backup Directory '%s' to S3", dirName)
	gplog.Verbose("S3 Location = s3://%s/%s", bucket, dirName)
	fmt.Printf("dirKey = %s\n", dirName)

	// Populate a list of files to be backed up
	fileList := make([]string, 0)
	_ = filepath.Walk(dirName, func(path string, f os.FileInfo, err error) error {
		isDir, _ := isDirectoryGetSize(path)
		if !isDir {
			fileList = append(fileList, path)
		}
		return nil
	})

	var wg sync.WaitGroup
	var finalErr error
	// Create jobs using a channel
	fileChannel := make(chan string, len(fileList))
	for _, fileKey := range fileList {
		wg.Add(1)
		fileChannel <- fileKey
	}
	close(fileChannel)
	// Process the files in parallel
	for i := 0; i < parallel; i++ {
		go func(jobs chan string) {
			for fileKey := range jobs {
				file, err := os.Open(fileKey)
				if err != nil {
					finalErr = err
					return
				}
				bytes, elapsed, err := uploadFile(sess, bucket, fileKey, file)
				if err == nil {
					totalBytes += bytes
					msg := fmt.Sprintf("Uploaded %d bytes for %s in %v", bytes,
						filepath.Base(fileKey), elapsed.Round(time.Millisecond))
					gplog.Verbose(msg)
					fmt.Println(msg)
				} else {
					finalErr = err
					gplog.FatalOnError(err)
				}
				_ = file.Close()
				wg.Done()
			}
		}(fileChannel)
	}
	// Wait for jobs to be done
	wg.Wait()

	fmt.Printf("Uploaded %d files (%d bytes) in %v\n",
		len(fileList), totalBytes, time.Since(start).Round(time.Millisecond))
	return finalErr
}

func BackupData(c *cli.Context) error {
	config, sess, err := readConfigAndStartSession(c, Gpbackup)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	bucket := config.Options["bucket"]
	fileKey := GetS3Path(config.Options["folder"], dataFile)
	bytes, elapsed, err := uploadFile(sess, bucket, fileKey, os.Stdin)
	if err == nil {
		gplog.Verbose("Uploaded %d bytes for file %s in %v", bytes,
			filepath.Base(fileKey), elapsed.Round(time.Millisecond))
	} else {
		gplog.FatalOnError(err)
	}
	return err
}

const UploadChunkSize = int64(Mebibyte) * 10

func uploadFile(sess *session.Session, bucket string, fileKey string,
	file *os.File) (int64, time.Duration, error) {

	start := time.Now()
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = UploadChunkSize
		u.Concurrency = Concurrency
	})
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
		Body:   bufio.NewReaderSize(file, int(UploadChunkSize) * Concurrency),
	})
	if err != nil {
		return 0, -1, err
	}
	bytes, err := getFileSize(uploader.S3, bucket, fileKey)
	return bytes, time.Since(start), err
}
