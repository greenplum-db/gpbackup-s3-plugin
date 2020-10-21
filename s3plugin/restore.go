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
	"github.com/inhies/go-bytesize"
	"github.com/urfave/cli"
)

func SetupPluginForRestore(c *cli.Context) error {
	scope := (Scope)(c.Args().Get(2))
	if scope != Master && scope != SegmentHost {
		return nil
	}
	_, err := readAndValidatePluginConfig(c.Args().Get(0))
	return err
}

func RestoreFile(c *cli.Context) error {
	config, sess, err := readConfigAndStartSession(c, Gprestore)
	if err != nil {
		return err
	}
	fileName := c.Args().Get(1)
	bucket := config.Options["bucket"]
	fileKey := GetS3Path(config.Options["folder"], fileName)
	file, err := os.Create(fileName)
	defer file.Close()
	if err != nil {
		return err
	}
	bytes, elapsed, err := downloadFile(sess, config, bucket, fileKey, file)
	if err != nil {
		fileErr := os.Remove(fileName)
		if fileErr != nil {
			gplog.Error(fileErr.Error())
		}
		return err
	}

	gplog.Info("Downloaded %d bytes for %s in %v", bytes,
		filepath.Base(fileKey), elapsed.Round(time.Millisecond))
	return err
}

func RestoreDirectory(c *cli.Context) error {
	start := time.Now()
	totalBytes := int64(0)
	config, sess, err := readConfigAndStartSession(c, Gprestore)
	if err != nil {
		return err
	}
	dirName := c.Args().Get(1)
	bucket := config.Options["bucket"]
	gplog.Verbose("Restore Directory '%s' from S3", dirName)
	gplog.Verbose("S3 Location = s3://%s/%s", bucket, dirName)
	gplog.Info("dirKey = %s\n", dirName)

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

		bytes, elapsed, err := downloadFile(sess, config, bucket, *key.Key, file)
		_ = file.Close()
		if err != nil {
			fileErr := os.Remove(filename)
			if fileErr != nil {
				gplog.Error(fileErr.Error())
			}
			return err
		}

		totalBytes += bytes
		numFiles++
		gplog.Info("Downloaded %d bytes for %s in %v", bytes,
			filepath.Base(*key.Key), elapsed.Round(time.Millisecond))
	}

	gplog.Info("Downloaded %d files (%d bytes) in %v\n",
		numFiles, totalBytes, time.Since(start).Round(time.Millisecond))
	return nil
}

func RestoreDirectoryParallel(c *cli.Context) error {
	start := time.Now()
	totalBytes := int64(0)
	parallel := 5
	config, sess, err := readConfigAndStartSession(c, Gprestore)
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
				bytes, elapsed, err := downloadFile(sess, config, bucket, fileKey, file)
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
	config, sess, err := readConfigAndStartSession(c, Gprestore)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	bucket := config.Options["bucket"]
	fileKey := GetS3Path(config.Options["folder"], dataFile)
	bytes, elapsed, err := downloadFile(sess, config, bucket, fileKey, os.Stdout)
	if err != nil {
		return err
	}

	gplog.Verbose("Downloaded %d bytes for file %s in %v", bytes,
		filepath.Base(fileKey), elapsed.Round(time.Millisecond))
	return nil
}

type chunk struct {
	chunkIndex int
	startByte  int64
	endByte    int64
}

func downloadFile(sess *session.Session, config *PluginConfig, bucket string, fileKey string,
	file *os.File) (int64, time.Duration, error) {

	start := time.Now()
	downloadChunkSize, err := GetDownloadChunkSize(config)
	if err != nil {
		return 0, -1, err
	}
	downloadConcurrency, err := GetDownloadConcurrency(config)
	if err != nil {
		return 0, -1, err
	}
	downloader := s3manager.NewDownloader(sess)

	totalBytes, err := getFileSize(downloader.S3, bucket, fileKey)
	if err != nil {
		return 0, -1, err
	}
	gplog.Verbose("File %s size = %d bytes", filepath.Base(fileKey), totalBytes)
	if totalBytes <= downloadChunkSize {
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
		return downloadFileInParallel(downloader, downloadConcurrency, downloadChunkSize, totalBytes, bucket, fileKey,
			file)
	}
	return totalBytes, time.Since(start), err
}

/*
 * Performs ranged requests for the file while exploiting parallelism between the copy and download tasks
 */
func downloadFileInParallel(downloader *s3manager.Downloader, downloadConcurrency int, downloadChunkSize int64,
	totalBytes int64, bucket string, fileKey string, file *os.File) (int64, time.Duration, error) {

	var finalErr error
	start := time.Now()
	waitGroup := sync.WaitGroup{}
	numberOfChunks := int((totalBytes + downloadChunkSize - 1) / downloadChunkSize)
	bufferPointers := make([]*[]byte, numberOfChunks)
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
		endByte += downloadChunkSize
		if endByte >= totalBytes {
			endByte = totalBytes - 1
			done = true
		}
		jobs <- chunk{chunkIndex, startByte, endByte}
		waitGroup.Add(1)
	}

	// Create a pool of download workers (based on concurrency)
	numberOfWorkers := downloadConcurrency
	if numberOfChunks < downloadConcurrency {
		numberOfWorkers = numberOfChunks
	}
	downloadBuffers := make(chan []byte, numberOfWorkers)
	for i := 0; i < cap(downloadBuffers); i++ {
		buffer := make([]byte, downloadChunkSize)
		downloadBuffers <- buffer
	}

	for i := 0; i < numberOfWorkers; i++ {
		go func(id int) {
			for j := range jobs {
				buffer := <- downloadBuffers
				chunkStart := time.Now()
				byteRange := fmt.Sprintf("bytes=%d-%d", j.startByte, j.endByte)
				if j.endByte - j.startByte + 1 != downloadChunkSize {
					buffer = make([]byte, j.endByte - j.startByte + 1)
				}
				bufferPointers[j.chunkIndex] = &buffer
				chunkBytes, err := downloader.Download(
					aws.NewWriteAtBuffer(buffer),
					&s3.GetObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(fileKey),
						Range:  aws.String(byteRange),
					})
				if err != nil {
					finalErr = err
				}
				gplog.Debug("Worker %d Downloaded %d bytes (chunk %d) for %s in %v",
					id, chunkBytes, j.chunkIndex, filepath.Base(fileKey),
					time.Since(chunkStart).Round(time.Millisecond))
				copyChannel[j.chunkIndex] <- j.chunkIndex
			}
		}(i)
	}

	// Copy data from download buffers into the output stream sequentially
	go func() {
		for i := range copyChannel {
			currentChunk := <-copyChannel[i]
			chunkStart := time.Now()
			numBytes, err := file.Write(*bufferPointers[currentChunk])
			if err != nil {
				finalErr = err
			}
			gplog.Debug("Copied %d bytes (chunk %d) for %s in %v",
				numBytes, currentChunk, filepath.Base(fileKey),
				time.Since(chunkStart).Round(time.Millisecond))
			// Deallocate buffer
			downloadBuffers <- *bufferPointers[currentChunk]
			bufferPointers[currentChunk] = nil
			waitGroup.Done()
			close(copyChannel[i])
		}
	}()

	waitGroup.Wait()
	return totalBytes, time.Since(start), finalErr
}

func GetDownloadChunkSize(config *PluginConfig) (int64, error) {
	downloadChunkSize := DownloadChunkSize
	if sizeFromConfig, ok := config.Options["restore_multipart_chunksize"]; ok {
		size, err := bytesize.Parse(sizeFromConfig)
		if err != nil {
			return 0, err
		}
		downloadChunkSize = int64(size)
	}
	return downloadChunkSize, nil
}

func GetDownloadConcurrency(config *PluginConfig) (int, error) {
	downloadConcurrency := Concurrency
	if val, ok := config.Options["restore_max_concurrent_requests"]; ok {
		r, err := strconv.Atoi(val)
		if err != nil {
			return 0, err
		}
		downloadConcurrency = r
	}
	return downloadConcurrency, nil
}
