package s3plugin

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
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

func BackupFile(c *cli.Context) error {
	gplog.InitializeLogging("gpbackup", "")
	config, sess, err := readConfigAndStartSession(c)
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
	totalBytes, elapsed, err := uploadFile(sess, bucket, fileKey, file)
	if err == nil {
		msg := fmt.Sprintf("Uploaded %d bytes for %s in %v", totalBytes,
			filepath.Base(fileName), elapsed.Round(time.Millisecond))
		gplog.Verbose(msg)
		fmt.Println(msg)
	} else {
		gplog.FatalOnError(err)
	}
	return err
}

func BackupDirectory(c *cli.Context) error {
	gplog.InitializeLogging("gpbackup", "")
	start := time.Now()
	total := int64(0)
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	defer func() {
		fmt.Printf("Uploaded %d bytes in %v\n", total,
			time.Since(start).Round(time.Millisecond))
	}()
	dirName := c.Args().Get(1)
	bucket := config.Options["bucket"]
	gplog.Verbose("Restore Directory '%s' from S3", dirName)
	gplog.Verbose("S3 Location = s3://%s/%s", bucket, dirName)
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
		totalBytes, elapsed, err := uploadFile(sess, bucket, fileName, file)
		if err == nil {
			total += totalBytes
			msg := fmt.Sprintf("Uploaded %d bytes for %s in %v", totalBytes,
				filepath.Base(fileName), elapsed.Round(time.Millisecond))
			gplog.Verbose(msg)
			fmt.Println(msg)
		} else {
			gplog.FatalOnError(err)
		}
		_ = file.Close()
	}
	return err
}

func BackupDirectoryParallel(c *cli.Context) error {
	gplog.InitializeLogging("gpbackup", "")
	start := time.Now()
	total := int64(0)
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	defer func() {
		fmt.Printf("Uploaded %d bytes in %v\n", total,
			time.Since(start).Round(time.Millisecond))
	}()
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
				total += totalBytes
				msg := fmt.Sprintf("Uploaded %d bytes for %s in %v", totalBytes,
					filepath.Base(fileName), elapsed.Round(time.Millisecond))
				gplog.Verbose(msg)
				fmt.Println(msg)
			} else {
				finalErr = err
				gplog.FatalOnError(err)
			}
			_ = file.Close()
		}(fileName)
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
	bucket := config.Options["bucket"]
	fileKey := GetS3Path(config.Options["folder"], dataFile)
	totalBytes, elapsed, err := uploadFile(sess, bucket, fileKey, os.Stdin)
	if err == nil {
		gplog.Verbose("Uploaded %d bytes for file %s in %v", totalBytes,
			filepath.Base(fileKey), elapsed.Round(time.Millisecond))
	} else {
		gplog.FatalOnError(err)
	}
	return err
}

const UploadChunkSize = int64(Mebibyte) * 500

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
