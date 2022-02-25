package mysql

import (
	"strings"
	"time"

	"github.com/fredgan/go-utils/log"
)

type logEvent struct {
	dur time.Duration
	sql  string
}

type SqlLogger struct {
	logger    *log.Logger
	closed    chan bool
	chLogging chan logEvent
}

func NewSqlLogger(path string) (*SqlLogger, error) {
	handler, err := log.NewTimeRotatingFileHandler(path, log.WhenDay, 1)
	if err != nil {
		return nil, err
	}
	logger := log.New(handler, log.Ltime)
	self := &SqlLogger{logger: logger}
	self.closed = make(chan bool)
	self.chLogging = make(chan logEvent)
	go self.loop()
	return self, nil
}

func (s *SqlLogger) loop() {
	for {
		select {
		case <-s.closed:
			for i := 0; len(s.chLogging) > 0 && i < 10000; i++ {
				time.Sleep(time.Microsecond)
				s.logger.Warn("Waiting for SQL log closed 10sec, remain[%vms].", 10000-i)
			}
			s.logger.Warn("SQL log-file was closed")
			s.logger.Close()
			return
		case l := <-s.chLogging:
			l.sql = strings.Replace(l.sql, "\n", "", -1)
			l.sql = strings.Replace(l.sql, "%", "%%", -1)
			s.logger.Info("%s - %s", l.dur, l.sql)
		}
	}
}

func (s *SqlLogger) Write(dur time.Duration, sql string) {
	s.chLogging <- logEvent{dur: dur, sql: sql}
}

func (s *SqlLogger) Close() {
	select {
	case s.closed <- true:
		return
	default:
		return
	}
}
