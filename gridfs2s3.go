package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/globalsign/mgo"
	"log"
	"mime"
	"path/filepath"
	"strings"
	"time"
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
		log.Fatal(err)
	}
}

func uploadFile(mongoSession *mgo.Session, fileName string) {

	//fmt.Printf("Try get file %s from GridFS\n", fileName)
	// Get file from GridFS
	g := mongoSession.Clone()
	//gfs = mongoSession.DB(mongoUrl.Db).GridFS("fs")
	gfs := g.DB(mongoUrl.Db).GridFS("fs")
	var fileType string

	file, err := gfs.Open(fileName)
	check(err)

	//// Create tmp file
	//tmpFile, err := ioutil.TempFile("", "gridfs2s3-")
	//check(err)
	//
	//savedFile := tmpFile.Name()
	//defer os.Remove(savedFile) // clean up
	defer g.Close()
	defer file.Close()
	//
	//// Write to tmp file from GridFS
	//_, err = io.Copy(tmpFile, file)
	//check(err)
	//err = file.Close()
	//check(err)
	////fmt.Printf("Saved %s to %s\n", fileName, savedFile)
	//
	//tmpFile.Close()
	//
	//f, err := os.Open(savedFile)
	//check(err)

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1")},
	)
	check(err)

	// detect mime type
	if file.ContentType() == "" {
		fileType = mime.TypeByExtension(filepath.Ext(fileName))
	} else {
		fileType = file.ContentType()
	}

	// Create S3 service client
	uploader := s3manager.NewUploader(sess)
	uploadPath := fmt.Sprintf("%s/%s/%s/%s", workBucket.Project, workBucket.Environment, workBucket.Prefix, replacer.Replace(fileName))
	// Upload the file's body to S3 bucket as an object with the key being the
	// same as the filename.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(workBucket.bucketName),
		Key:         aws.String(filepath.Clean(uploadPath)),
		Body:        file,
		ContentType: aws.String(fileType),
	})
	check(err)
	log.Printf("%s saved to %s as %s\n", fileName, uploadPath, fileType)
}

func init() {
	flag.StringVar(&workBucket.Project, "project", "", "Project name")
	flag.StringVar(&workBucket.bucketName, "bucket", "", "S3 bucket")
	flag.StringVar(&workBucket.Environment, "env", "", "Project environment")
	flag.BoolVar(&optionsApp.version, "version", false, "Show version info")
	flag.StringVar(&workBucket.Prefix, "prefix", "uploads", "path prefix")
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
		if f.Name() != "" && !strings.Contains(f.Name(), "unison") {
			//fmt.Printf("Try upload file %s with ObjectId %s\n", f.Name(), f.Id())
			go uploadFile(sessionMongo, f.Name())
			<-time.After(time.Millisecond * 50)
		} else {
			log.Printf("Object with id ObjectId %s has no filename\n", f.Id())
		}
	}

	defer iter.Close()
	//if iter.Close() != nil {
	//	panic(iter.Close())
	//}

}
