package log

import (
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	s_timeLogger *timeRollLogger
	TimePrefix   = &struct{}{}
)

func TimeLogInit(logDir string) error {
	if err := os.MkdirAll(logDir, 0666); err != nil {
		return err
	}

	s_timeLogger = newTimeLogger(logDir)
	return nil
}

func TimeLogClose() {
	if s_timeLogger != nil {
		s_timeLogger.Close()
		s_timeLogger = nil
	}
}

func DLOG(fileName string, format string, v ...interface{}) {
	if s_timeLogger == nil {
		panic("s_timeLogger in not init, please call TimeLogInit")
	}
	s_timeLogger.WriteLog(&logEntity{
		fileName: fileName,
		format:   format,
		args:     v,
	})
}

type logEntity struct {
	fileName string
	format   string
	args     []interface{}
}

const (
	lineBuffSize = 2
)

type fileEntity struct {
	file *TimeRollFile
	buff []byte
	size int
}

type timeRollLogger struct {
	dFiles map[string]*fileEntity
	logDir string
	logs   chan *logEntity
	quit   chan struct{}
	wg     sync.WaitGroup
	closed bool
}

func newTimeLogger(logDir string) *timeRollLogger {
	t := &timeRollLogger{
		dFiles: make(map[string]*fileEntity),
		logDir: logDir,
		logs:   make(chan *logEntity, 4096),
		quit:   make(chan struct{}),
		closed: false,
	}

	go t.run()
	return t
}

func (self *timeRollLogger) run() {
	self.wg.Add(1)
	for {
		select {
		case log := <-self.logs:
			now := time.Now()
			self.writeLogToBuff(now, log)

			i := 1
			for ; len(self.logs) > 0; i++ {
				log = <-self.logs
				if i%1000 == 0 {
					now = time.Now()
				}
				self.writeLogToBuff(now, log)
			}

			self.writeBufferToFile(now.Unix())

			if i < 500 {
				time.Sleep(time.Second) //等1秒，进行聚合写
			}

		case <-self.quit:
			break
		}
	}

	self.close()
}

func (self *timeRollLogger) WriteLog(log *logEntity) {
	select {
	case self.logs <- log:

	case <-time.After(time.Millisecond * 10):
		Error("WriteLog timeout, %v", log)
	}
}

func (self *timeRollLogger) writeLogToBuff(now time.Time, log *logEntity) {
	en := self.getDayLogFile(log.fileName)

	nowSec := now.Unix()
	if len(log.args) > 0 && log.args[0] == TimePrefix {
		log.args[0] = now.Format("2006/01/02 15:04:05")
	} else {
		self.writeStringToBuff(en, nowSec, now.Format("2006/01/02 15:04:05 "))
	}

	content := fmt.Sprintf(log.format, log.args...)
	self.writeStringToBuff(en, nowSec, content)
	self.writeStringToBuff(en, nowSec, "\n")
}

func (self *timeRollLogger) writeStringToBuff(en *fileEntity, nowSec int64, s string) {
	for len(s) > lineBuffSize-en.size {
		n := copy(en.buff[en.size:], s)
		en.size += n
		s = s[n:]
		en.file.Write(nowSec, en.buff[0:en.size])
		en.size = 0
	}
	n := copy(en.buff[en.size:], s)
	en.size += n
}

func (self *timeRollLogger) writeBufferToFile(nowSec int64) {
	for _, en := range self.dFiles {
		if en.size > 0 {
			en.file.Write(nowSec, en.buff[0:en.size])
			en.size = 0
		}
	}
}

func (self *timeRollLogger) getDayLogFile(fileName string) *fileEntity {
	en, ok := self.dFiles[fileName]
	if ok {
		return en
	}

	en = &fileEntity{
		file: NewTimeRollFile(fileName, self.logDir, RollTypeDay),
		buff: make([]byte, lineBuffSize),
	}
	self.dFiles[fileName] = en
	return en
}

func (self *timeRollLogger) close() {
	for _, en := range self.dFiles {
		en.file.Close()
	}

	self.dFiles = nil
}

func (self *timeRollLogger) Close() {
	if self.closed {
		return
	}
	self.closed = true
	close(self.quit)
	self.wg.Wait()
	self.quit = nil
}
