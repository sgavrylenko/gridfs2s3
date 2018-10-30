package main

import (
	"fmt"
	"os"
)

type MongoDsn struct {
	Host string
	Port string
	Db   string
	User string
	Pass string
}

func (dsn *MongoDsn) init() {
	var ok bool
	dsn.Host, ok = os.LookupEnv("MONGOHOST")
	if !ok {
		dsn.Host = "localhost"
	}
	dsn.Port, ok = os.LookupEnv("MONGOPORT")
	if !ok {
		dsn.Port = "27017"
	}
	dsn.Db, ok = os.LookupEnv("MONGODB")
	if !ok {
		dsn.Db = "uploads"
	}
	dsn.User = os.Getenv("MONGOUSER")
	dsn.Pass = os.Getenv("MONGOPASS")
}

func (dsn *MongoDsn) buildDsn() string {
	var mongoStr string
	if dsn.User != "" && dsn.Pass != "" {
		mongoStr = fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", dsn.User, dsn.Pass, dsn.Host, dsn.Port, dsn.Db)
	} else {
		mongoStr = fmt.Sprintf("mongodb://%s:%s/%s", dsn.Host, dsn.Port, dsn.Db)
	}
	return mongoStr
}

type BucketInfo struct {
	bucketName  string
	Project     string
	Environment string
	Prefix      string
}

func (info *BucketInfo) checkParams() {
	if info.bucketName == "" {
		fmt.Println("You need to set bucket name with parameter -bucket")
		os.Exit(1)
	}
	if info.Project == "" {
		fmt.Println("You need to set project name with parameter -project")
		os.Exit(1)
	}
	if info.Environment == "" {
		fmt.Println("You need to set env name with parameter -env")
		os.Exit(1)
	}
}

type CommonOptions struct {
	version bool
}

func (opts *CommonOptions) checkParams() {
	if opts.version {
		fmt.Printf("Git Repo: %s\nGit Commit Hash: %s\nVersion: v%s\nBuild Date: %s\n", gitRepo, gitCommit, appVersion, buildStamp)
		os.Exit(0)
	}
}
