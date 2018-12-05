package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type stop struct {
	error
}

func main() {
	start := time.Now()
	bucket := aws.String(os.Args[2])
	svc := s3.New(session.Must(session.NewSession(&aws.Config{Region: aws.String(os.Args[1])})))

	input := &s3.ListObjectsV2Input{
		Bucket:  bucket,
		MaxKeys: aws.Int64(1000),
	}

	pageNum := 0
	keys := make(map[int][]string)

	fmt.Println("listing objects")

	err := svc.ListObjectsV2Pages(input,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			pageNum++
			pagedKeys := []string{}
			for _, value := range page.Contents {
				pagedKeys = append(pagedKeys, *value.Key)
			}
			keys[pageNum] = append(keys[pageNum], pagedKeys...)
			fmt.Printf("keys to delete: %v\n", len(keys)*1000)
			return true
		})
	if err != nil {
		log.Println("i had an error so im stopping here and deleting what i have" + err.Error())
	}

	fmt.Printf("time to list %v objects: %v\n", len(keys)*1000, time.Since(start))

	var wg sync.WaitGroup

	for _, allocKeys := range keys {
		wg.Add(1)
		go func(svc *s3.S3, keys []string, bucket *string) error {
			defer wg.Done()
			fmt.Printf("deleting: %v\n", len(keys))
			awsKeys := make([]*s3.ObjectIdentifier, 0, 1000)
			for _, key := range keys {
				awsKey := s3.ObjectIdentifier{
					Key: aws.String(key),
				}
				awsKeys = append(awsKeys, &awsKey)
			}
			input := &s3.DeleteObjectsInput{
				Bucket: bucket,
				Delete: &s3.Delete{
					Objects: awsKeys,
				},
			}
			return retry(30, time.Second, func() error {
				result, err := svc.DeleteObjects(input)
				if err != nil {
					if aerr, ok := err.(awserr.Error); ok {
						switch aerr.Code() {
						default:
							return aerr
						}
					} else {
						return err
					}
				}
				fmt.Printf("deleted %v objects\n", len(result.Deleted))
				return nil
			})
		}(svc, allocKeys, bucket)
	}
	wg.Wait()
	fmt.Printf("time to delete %v objects: %v\n", len(keys)*1000, time.Since(start))
}

func retry(attempts int, sleep time.Duration, f func() error) error {
	if err := f(); err != nil {
		if s, ok := err.(stop); ok {
			// Return the original error for later checking
			return s.error
		}

		if attempts--; attempts > 0 {
			// Add some randomness to prevent creating a Thundering Herd
			jitter := time.Duration(rand.Int63n(int64(sleep)))
			sleep = sleep + jitter/2

			time.Sleep(sleep)
			return retry(attempts, 2*sleep, f)
		}
		return err
	}

	return nil
}
