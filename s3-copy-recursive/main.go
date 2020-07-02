package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var public = flag.Bool("public", false, "source bucket is public")
var srcPrefix = flag.String("src-prefix", "", "source prefix to copy")
var destPrefix = flag.String("dest-prefix", "", "destination prefix")

func main() {
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s SRC_BUCKET DEST_BUCKET\n", filepath.Base(os.Args[0]))
		os.Exit(2)
	}
	srcBucket := flag.Arg(0)
	destBucket := flag.Arg(1)

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

	err = srcS3Svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: &srcBucket,
		Prefix: srcPrefix,
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, object := range page.Contents {
			// Skip directory objects created by S3 console
			if strings.HasSuffix(*object.Key, "/") {
				continue
			}

			fmt.Printf("Copying '%s'\n", *object.Key)

			destKey := fmt.Sprintf("%s/%s", *destPrefix, strings.TrimPrefix(strings.TrimPrefix(*object.Key, *srcPrefix), "/"))

			objectData, err := srcS3Svc.GetObject(&s3.GetObjectInput{
				Bucket: &srcBucket,
				Key:    object.Key,
			})
			if err != nil {
				panic(err)
			}

			_, err = destS3Uploader.Upload(&s3manager.UploadInput{
				Bucket:      &destBucket,
				Key:         &destKey,
				Body:        objectData.Body,
				ContentType: objectData.ContentType,
			})
			objectData.Body.Close()
			if err != nil {
				panic(err)
			}
		}

		return true
	})
	if err != nil {
		panic(err)
	}
}
