package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gopkg.in/mgo.v2"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

var (
	gitRepo    = "No repo info"
	gitCommit  = "No Hash Provided"
	appVersion = "No Version Provided"
	buildStamp = "No Time Provided"
)

var mongoUrl MongoDsn
var workBucket BucketInfo
var optionsApp CommonOptions

var replacer = strings.NewReplacer("_", "/")

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func uploadFile(mongoSession *mgo.Session, fileName string) {

	//fmt.Printf("Try get file %s from GridFS\n", fileName)
	// Get file from GridFS
	gfs := mongoSession.DB(mongoUrl.Db).GridFS("fs")
	file, err := gfs.Open(fileName)
	check(err)

	// Create tmp file
	tmpFile, err := ioutil.TempFile("", "gridfs2s3-")
	check(err)

	savedFile := tmpFile.Name()
	defer os.Remove(savedFile) // clean up

	// Write to tmp file from GridFS
	_, err = io.Copy(tmpFile, file)
	check(err)
	err = file.Close()
	check(err)
	//fmt.Printf("Saved %s to %s\n", fileName, savedFile)

	tmpFile.Close()

	f, err := os.Open(savedFile)
	check(err)

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1")},
	)
	check(err)

	// Create S3 service client
	uploader := s3manager.NewUploader(sess)
	uploadPath := fmt.Sprintf("%s/%s/%s", workBucket.Project, workBucket.Environment, replacer.Replace(fileName))
	// Upload the file's body to S3 bucket as an object with the key being the
	// same as the filename.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(workBucket.bucketName),
		Key:    aws.String(filepath.Clean(uploadPath)),
		Body:   f,
	})
	check(err)
	fmt.Printf("%s saved to %s\n", fileName, uploadPath)
}

func init() {
	flag.StringVar(&workBucket.Project, "project", "", "Project name")
	flag.StringVar(&workBucket.bucketName, "bucket", "", "S3 bucket")
	flag.StringVar(&workBucket.Environment, "env", "", "Project environment")
	flag.BoolVar(&optionsApp.version, "version", false, "Show version info")
	flag.Parse()
	optionsApp.checkParams()
	workBucket.checkParams()
	mongoUrl.init()
}

func main() {

	sessionMongo, err := mgo.Dial(mongoUrl.buildDsn())
	if err != nil {
		panic(err)
	}
	defer sessionMongo.Close()

	// Optional. Switch the session to a monotonic behavior.
	sessionMongo.SetMode(mgo.Monotonic, true)

	// Search all files in GridFS
	gfs := sessionMongo.DB(mongoUrl.Db).GridFS("fs")
	iter := gfs.Find(nil).Iter()

	var f *mgo.GridFile

	for gfs.OpenNext(iter, &f) {
		if f.Name() != "" {
			//fmt.Printf("Try upload file %s with ObjectId %s\n", f.Name(), f.Id())
			uploadFile(sessionMongo, f.Name())
		} else {
			fmt.Printf("Object with id ObjectId %s has no filename\n", f.Id())
		}
	}

	if iter.Close() != nil {
		panic(iter.Close())
	}

}
