package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var public = flag.Bool("public", false, "source bucket is public")

func main() {
	flag.Parse()

	if flag.NArg() < 3 {
		fmt.Fprintf(os.Stderr, "usage: %s SRC_BUCKET SRC_KEY DEST_BUCKET\n", filepath.Base(os.Args[0]))
		os.Exit(2)
	}
	srcBucket := flag.Arg(0)
	srcKey := flag.Arg(1)
	destBucket := flag.Arg(2)

	sess, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	srcRegion, err := s3manager.GetBucketRegion(context.TODO(), sess, srcBucket, "")
	if err != nil {
		panic(err)
	}

	srcConfig := &aws.Config{
		Region: &srcRegion,
	}

	if *public {
		srcConfig.Credentials = credentials.AnonymousCredentials
	}

	srcS3Svc := s3.New(sess, srcConfig)
	destS3Uploader := s3manager.NewUploader(sess)

	i := 1
	for {
		fmt.Printf("Iteration #%d\n", i)

		objectData, err := srcS3Svc.GetObject(&s3.GetObjectInput{
			Bucket: &srcBucket,
			Key:    &srcKey,
		})
		if err != nil {
			panic(err)
		}

		_, err = destS3Uploader.Upload(&s3manager.UploadInput{
			Bucket:      &destBucket,
			Key:         aws.String(fmt.Sprintf("s3-download-upload-stress/%s-%d", srcKey, i)),
			Body:        objectData.Body,
			ContentType: objectData.ContentType,
		})
		objectData.Body.Close()
		if err != nil {
			panic(err)
		}

		i++
	}
}
