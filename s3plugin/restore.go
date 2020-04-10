package s3plugin

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/urfave/cli"
)

func SetupPluginForRestore(c *cli.Context) error {
	scope := (Scope)(c.Args().Get(2))
	if scope != Master && scope != SegmentHost {
		return nil
	}
	gplog.InitializeLogging("gprestore", "")
	_, err := readAndValidatePluginConfig(c.Args().Get(0))
	return err
}

func RestoreFile(c *cli.Context) error {
	gplog.InitializeLogging("gprestore", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	fileName := c.Args().Get(1)
	bucket := config.Options["bucket"]
	fileKey := GetS3Path(config.Options["folder"], fileName)
	file, err := os.Create(fileName)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}
	bytes, elapsed, err := downloadFile(sess, bucket, fileKey, file)
	if err == nil {
		msg := fmt.Sprintf("Downloaded %d bytes for %s in %v", bytes,
			filepath.Base(fileKey), elapsed.Round(time.Millisecond))
		gplog.Verbose(msg)
		fmt.Println(msg)
	} else {
		gplog.FatalOnError(err)
		_ = os.Remove(fileName)
	}
	return err
}

func RestoreDirectory(c *cli.Context) error {
	gplog.InitializeLogging("gprestore", "")
	start := time.Now()
	totalBytes := int64(0)
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dirName := c.Args().Get(1)
	bucket := config.Options["bucket"]
	gplog.Verbose("Restore Directory '%s' from S3", dirName)
	gplog.Verbose("S3 Location = s3://%s/%s", bucket, dirName)
	fmt.Printf("dirKey = %s\n", dirName)

	_ = os.MkdirAll(dirName, 0775)
	client := s3.New(sess)
	params := &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &dirName}
	bucketObjectsList, _ := client.ListObjectsV2(params)

	numFiles := 0
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
		bytes, elapsed, err := downloadFile(sess, bucket, *key.Key, file)
		if err == nil {
			totalBytes += bytes
			numFiles++
			msg := fmt.Sprintf("Downloaded %d bytes for %s in %v", bytes,
				filepath.Base(*key.Key), elapsed.Round(time.Millisecond))
			gplog.Verbose(msg)
			fmt.Println(msg)
		} else {
			gplog.FatalOnError(err)
			_ = os.Remove(filename)
		}
		_ = file.Close()
	}

	fmt.Printf("Downloaded %d files (%d bytes) in %v\n",
		numFiles, totalBytes, time.Since(start).Round(time.Millisecond))
	return err
}

func RestoreDirectoryParallel(c *cli.Context) error {
	gplog.InitializeLogging("gprestore", "")
	start := time.Now()
	totalBytes := int64(0)
	parallel := 5
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dirName := c.Args().Get(1)
	if len(c.Args()) == 3 {
		parallel, _ = strconv.Atoi(c.Args().Get(2))
	}
	bucket := config.Options["bucket"]
	gplog.Verbose("Restore Directory Parallel '%s' from S3", dirName)
	gplog.Verbose("S3 Location = s3://%s/%s", bucket, dirName)
	fmt.Printf("dirKey = %s\n", dirName)

	_ = os.MkdirAll(dirName, 0775)
	client := s3.New(sess)
	params := &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &dirName}
	bucketObjectsList, _ := client.ListObjectsV2(params)

	// Create a list of files to be restored
	numFiles := 0
	fileList := make([]string, 0)
	for _, key := range bucketObjectsList.Contents {
		gplog.Verbose("File '%s' = %d bytes", filepath.Base(*key.Key), *key.Size)
		if strings.HasSuffix(*key.Key, "/") {
			// Got a directory
			continue
		}
		fileList = append(fileList, *key.Key)
	}

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
				bytes, elapsed, err := downloadFile(sess, bucket, fileKey, file)
				if err == nil {
					totalBytes += bytes
					numFiles++
					msg := fmt.Sprintf("Downloaded %d bytes for %s in %v", bytes,
						filepath.Base(fileKey), elapsed.Round(time.Millisecond))
					gplog.Verbose(msg)
					fmt.Println(msg)
				} else {
					finalErr = err
					gplog.FatalOnError(err)
					_ = os.Remove(filePath)
				}
				_ = file.Close()
				wg.Done()
			}
		}(fileChannel)
	}
	// Wait for jobs to be done
	wg.Wait()

	fmt.Printf("Downloaded %d files (%d bytes) in %v\n",
		numFiles, totalBytes, time.Since(start).Round(time.Millisecond))
	return finalErr
}

func RestoreData(c *cli.Context) error {
	gplog.InitializeLogging("gprestore", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	bucket := config.Options["bucket"]
	fileKey := GetS3Path(config.Options["folder"], dataFile)
	bytes, elapsed, err := downloadFile(sess, bucket, fileKey, os.Stdout)
	if err == nil {
		gplog.Verbose("Downloaded %d bytes for file %s in %v", bytes,
			filepath.Base(fileKey), elapsed.Round(time.Millisecond))
	} else {
		gplog.FatalOnError(err)
	}
	return err
}

/*
   8 MB starting chunk where each subsequent chunk increments by 1MB
   AWS only allows upto 10000 chunks with Max file size of 5TB
   The limit would be reached at chunk number 3155 in our case
   Chunk sizes = 8, 9, 10, ..., 3155
*/
const DownloadChunkSize = int64(Mebibyte) * 8
const DownloadChunkIncrement = int64(Mebibyte) * 1

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
		buffer := &aws.WriteAtBuffer{}
		if _, err = downloader.Download(
			buffer,
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(fileKey),
			}); err != nil {
			return 0, -1, err
		}
		if _, err = file.Write(buffer.Bytes()); err != nil {
			return 0, -1, err
		}
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
	downloadBuffers := make([][]byte, numberOfChunks)
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
		endByte += DownloadChunkSize + int64(chunkIndex)*DownloadChunkIncrement
		if endByte >= totalBytes {
			endByte = totalBytes - 1
			done = true
		}
		jobs <- chunk{chunkIndex, startByte, endByte}
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
				var chunkStart time.Time
				if gplog.GetLogFileVerbosity() >= gplog.LOGVERBOSE {
					chunkStart = time.Now()
				}
				byteRange := fmt.Sprintf("bytes=%d-%d", j.startByte, j.endByte)
				// Allocate buffer for download
				downloadBuffers[j.chunkIndex] = make([]byte, j.endByte-j.startByte+1)
				chunkBytes, err := downloader.Download(
					aws.NewWriteAtBuffer(downloadBuffers[j.chunkIndex]),
					&s3.GetObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(fileKey),
						Range:  aws.String(byteRange),
					})
				if err != nil {
					finalErr = err
				}
				if gplog.GetLogFileVerbosity() >= gplog.LOGVERBOSE {
					gplog.Verbose("Worker %d Downloaded %d bytes (chunk %d) for %s in %v",
						id, chunkBytes, j.chunkIndex, filepath.Base(fileKey),
						time.Since(chunkStart).Round(time.Millisecond))
				}
				copyChannel[j.chunkIndex] <- j.chunkIndex
				time.Sleep(time.Millisecond * 10)
			}
		}(i)
	}

	// Copy data from download buffers into the output stream sequentially
	go func() {
		for i := range copyChannel {
			currentChunk := <-copyChannel[i]
			var chunkStart time.Time
			if gplog.GetLogFileVerbosity() >= gplog.LOGVERBOSE {
				chunkStart = time.Now()
			}
			numBytes, err := file.Write(downloadBuffers[currentChunk])
			if err != nil {
				finalErr = err
			}
			if gplog.GetLogFileVerbosity() >= gplog.LOGVERBOSE {
				gplog.Verbose("Copied %d bytes (chunk %d) for %s in %v",
					numBytes, currentChunk, filepath.Base(fileKey),
					time.Since(chunkStart).Round(time.Millisecond))
			}
			// Deallocate buffer
			downloadBuffers[currentChunk] = nil
			waitGroup.Done()
			close(copyChannel[i])
		}
	}()

	waitGroup.Wait()
	return totalBytes, time.Since(start), finalErr
}
