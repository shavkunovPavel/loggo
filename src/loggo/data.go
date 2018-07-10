package main

import (
	"crypto/sha1"
	"encoding/base64"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"regexp"
	"time"
)

type LogItem struct {
	LogTime   time.Time `bson:"log_time"`
	LogMsg    string    `bson:"log_msg"`
	FileName  string    `bson:"file_name"`
	LogFormat string    `bson:"log_format"`
	Id        string    `bson:"_id"`
}

func createLogItem(stringItem string, file string, format string) (li *LogItem) {
	li = new(LogItem)

	re := regexp.MustCompile(`(.*[^\s])(\s*\|\s*)([^\s].*)`)
	match := re.FindStringSubmatch(stringItem)

	if !li.setTimeRFC3339(match[1]) {
		li.setTimeForm(match[1])
	}
	li.setId(match[1] + match[3])
	li.LogMsg = match[3]
	li.FileName = file
	li.LogFormat = format

	return
}

func (li *LogItem) setId(field string) {
	hasher := sha1.New()
	hasher.Write([]byte(field))
	li.Id = base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

func (li *LogItem) setTime(format string, timeString string) bool {
	liTime, err := time.Parse(format, timeString)
	if err != nil {
		return false
	}
	li.LogTime = liTime
	return true
}

func (li *LogItem) setTimeRFC3339(timeString string) bool {
	return li.setTime(time.RFC3339, timeString)
}

func (li *LogItem) setTimeForm(timeString string) bool {
	return li.setTime("Jan 2, 2006 at 3:04:05pm (UTC)", timeString)
}

func (li *LogItem) insert(c *mgo.Collection) (err error) {
	err = c.Find(bson.M{"_id": li.Id}).One(new(LogItem))
	if err != nil && err.Error() == "not found" {
		err = c.Insert(li)
	}
	return
}
