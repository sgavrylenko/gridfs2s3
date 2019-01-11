package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"log"
	"math/rand"
	"mime"
	"path/filepath"
	"runtime"
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
var maxRetry uint

//var SessionMongo *mgo.Session

var replacer = strings.NewReplacer("_", "/")

const goroutinesNum = 12

//type MongoFileInfo struct {
//	SessionMongo *mgo.Session
//	GFile        *mgo.GridFile
//}

type Element struct {
	Id          bson.ObjectId `bson:"_id"`
	Filename    string        `bson:",omitempty"`
	ContentType string        `bson:"contentType,omitempty"`
	UploadDate  time.Time     `bson:"uploadDate"`
	Length      int64         `bson:",minsize"`
	ItemNum     int32         `bson:"itemNum,omitempty"`
	ItemTotal   int32         `bson:"itemTotal,omitempty"`
	RetryCount  int           `bson:",omitempty"`
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func ByteCountBinary(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func wrapUploadFile(connection *mgo.Session, fileInfo Element) {
	var delay int64
	var retryCount uint
	maxRetry = 5
	//delay = 0

	for retryCount = 0; retryCount < maxRetry; retryCount++ {
		if retryCount != 0 {
			delay = int64(rand.Intn(1000))
		}
		time.Sleep(time.Duration(delay) * time.Millisecond)
		fileInfo.RetryCount = int(retryCount)
		err := uploadFile(connection, fileInfo)

		if err != nil {
			log.Printf("Critical: %s; try upload %s, retry %d", err, fileInfo.Filename, retryCount)
			continue
		} else {
			break
		}
	}

	if retryCount == maxRetry {
		log.Printf("Critical: error upload %s", fileInfo.Filename)
	}
}

func uploadFile(connection *mgo.Session, fileInfo Element) error {

	startAt := time.Now()
	gfs := connection.DB(mongoUrl.Db).GridFS("fs")

	file, err := gfs.Open(fileInfo.Filename)
	check(err)

	defer file.Close()

	var fileType string
	if file.ContentType() == "" {
		fileType = mime.TypeByExtension(filepath.Ext(fileInfo.Filename))
	} else {
		fileType = file.ContentType()
	}

	sessAws, err := session.NewSession(&aws.Config{
		Region:     aws.String("eu-central-1"),
		MaxRetries: aws.Int(5)},
	)
	check(err)

	uploader := s3manager.NewUploader(sessAws)

	uploadPath := fmt.Sprintf("%s/%s/%s/%s", workBucket.Project, workBucket.Environment, workBucket.Prefix, replacer.Replace(fileInfo.Filename))
	// Upload the file's body to S3 bucket as an object with the key being the
	// same as the filename.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(workBucket.bucketName),
		Key:         aws.String(filepath.Clean(uploadPath)),
		Body:        file,
		ContentType: aws.String(fileType),
	})

	if err != nil {
		log.Printf("Error: %s, retry count: %d", err, fileInfo.RetryCount)
		return err
	}

	log.Printf("%d/%d Done: %s in %.2f secs, size %s", fileInfo.ItemNum, fileInfo.ItemTotal, result.Location, time.Since(startAt).Seconds(), ByteCountBinary(fileInfo.Length))
	//time.Sleep(200*time.Millisecond)
	return nil
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

func deduplicate(elements []Element) []Element {
	keys := make(map[string]bool)
	list := make([]Element, 0, len(elements))

	for _, entry := range elements {
		if _, present := keys[entry.Filename]; !present {
			keys[entry.Filename] = true
			list = append(list, entry)
		}
	}
	return list
}

func startWorker(session *mgo.Session, workerNum int, in <-chan Element) {
	log.Printf("Info: worker %d started\n", workerNum+1)
	connection := session.Copy()
	defer connection.Close()

	for input := range in {
		//fmt.Printf("Worker %d\tfile: %s\n",workerNum,input.FileName)
		wrapUploadFile(connection, input)
		runtime.Gosched()
	}
	log.Printf("Info: worker %d stoped\n", workerNum+1)
}

func main() {

	startAt := time.Now()
	runtime.GOMAXPROCS(0)

	worketInput := make(chan Element, 2)

	SessionMongo, err := mgo.Dial(mongoUrl.buildDsn())
	if err != nil {
		panic(err)
	}
	defer SessionMongo.Close()

	// Optional. Switch the session to a monotonic behavior.
	SessionMongo.SetMode(mgo.Monotonic, true)

	for i := 0; i < goroutinesNum; i++ {
		go startWorker(SessionMongo, i, worketInput)
	}

	mainSession := SessionMongo.Copy()

	var results []Element
	// Search all files in GridFS
	gfs := mainSession.DB(mongoUrl.Db).GridFS("fs")
	iter := gfs.Find(nil).Sort("filename").Iter()

	err = iter.All(&results)
	if err != nil {
		log.Println(err)
	}

	normalizedResults := deduplicate(results)

	var itemNum int32
	var itemTotal int32
	var normalizedTotal int32
	var goodCount int32
	var wrongCount int32

	goodCount = 0
	itemTotal = int32(len(results))
	normalizedTotal = int32(len(normalizedResults))
	wrongCount = 0

	log.Println("Info: find ", itemTotal, "objects")

	// destroy results slice for free memory
	results = nil

	for _, element := range normalizedResults {
		itemNum += 1
		if element.Filename != "" && !strings.Contains(element.Filename, "unison") {
			goodCount += 1
			element.ItemTotal = normalizedTotal
			element.ItemNum = itemNum
			worketInput <- element
		} else {
			wrongCount += 1
			log.Printf("%d/%d Error: %s has no filename\n", itemNum, itemTotal, element.Id.Hex())
		}
	}

	//var f *mgo.GridFile
	//
	//for gfs.OpenNext(iter, &f) {
	//	if f.Name() != "" && !strings.Contains(f.Name(), "unison") {
	//		worketInput <- MongoFileInfo{SessionMongo, f}
	//	} else {
	//		log.Printf("Error: %s has no filename\n", f.Id())
	//	}
	//	if iter.Timeout() {
	//		log.Println("Info: cursor timeout occurred")
	//		continue
	//	}
	//	if iter.Err() != nil {
	//		log.Printf("Error: %s, Id %s, file %s\n", iter.Err(), f.Id(), f.Name())
	//		iter.Close()
	//	}
	//}

	close(worketInput)
	time.Sleep(20 * time.Second)

	if iter.Close() != nil {
		panic(iter.Close())
	}
	log.Printf("Total obects in DB: %d", itemTotal)
	log.Printf("Total files for upload: %d", normalizedTotal)
	log.Printf("Wrong files: %d", wrongCount)
	log.Printf("Uploaded files: %d", goodCount)
	log.Printf("Total time: %.1f minutes", time.Since(startAt).Minutes())
}
