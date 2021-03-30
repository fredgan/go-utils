package log

import (
	"os"
	"time"
)

type TimeRollFile struct {
	fileName    string
	fileDir     string
	file        *os.File
	curFileName string

	starTime int64
	endTime  int64

	rollTimeTypeCfg rollTimeTypeConfig
}

type rollTimeTypeConfig struct {
	timeFormat string
	duration   int64
}

var rollTimeTypeCfgs = []rollTimeTypeConfig{
	{
		timeFormat: "20060102",
		duration:   24 * 3600,
	},
}

const (
	RollTypeDay = iota
)

func NewTimeRollFile(fileName string, fileDir string, rollTimeType int) *TimeRollFile {
	return &TimeRollFile{
		fileName:        fileName,
		fileDir:         fileDir,
		starTime:        0,
		endTime:         0,
		rollTimeTypeCfg: rollTimeTypeCfgs[rollTimeType],
	}
}

func (self *TimeRollFile) Write(logTime int64, content []byte) error {

	f, err := self.getCurFile(logTime)
	if err != nil {
		return err
	}

	if _, err := f.Write(content); err != nil {
		Error("[error]write file error, content=%s, filename=%s\n", content, self.curFileName)
	}
	return nil
}

func (self *TimeRollFile) WriteString(logTime int64, content string) error {

	f, err := self.getCurFile(logTime)
	if err != nil {
		return err
	}

	if _, err := f.WriteString(content); err != nil {
		Error("[error]write file error, content=%s, filename=%s\n", content, self.curFileName)
	}
	return nil
}

func (self *TimeRollFile) Close() {
	if self.file != nil {
		if err := self.file.Close(); err != nil {
			Error("[info]elf.osFile.Close() %s, err=%s", self.curFileName, err.Error())
		}

		self.file = nil
		Info("[info]close file %s", self.curFileName)
	}
}

func (self *TimeRollFile) getCurFile(logTime int64) (*os.File, error) {
	bNeedSwitch := logTime < self.starTime || self.endTime <= logTime
	//switch file
	if bNeedSwitch {
		self.Close()
		self.setCurTime(logTime)
	}

	if self.file == nil {
		f, err := self.getNewFile(self.starTime)
		if err != nil {
			return nil, err
		}
		self.file = f
	}

	return self.file, nil
}

func (self *TimeRollFile) setCurTime(logTime int64) {
	t := time.Unix(logTime, 0)

	timeStr := t.Format(self.rollTimeTypeCfg.timeFormat)
	startTime, _ := time.ParseInLocation(self.rollTimeTypeCfg.timeFormat, timeStr, t.Location())

	self.starTime = startTime.Unix()
	self.endTime = self.starTime + self.rollTimeTypeCfg.duration
}

func (self *TimeRollFile) getFilePath(sec int64) string {
	t := time.Unix(sec, 0)
	timeFormat := t.Format(self.rollTimeTypeCfg.timeFormat)
	return self.fileDir + self.fileName + "_" + timeFormat + ".log"
}

func (self *TimeRollFile) getNewFile(logTime int64) (*os.File, error) {
	self.curFileName = self.getFilePath(logTime)

	f, err := os.OpenFile(self.curFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		Error("[info]open file %s, error=%v\n", self.curFileName, err)
		return nil, err
	}

	Debug("[info]open file %s\n", self.curFileName)

	return f, nil
}
