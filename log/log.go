package log

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

//log level, from low to high, more high means more serious
const (
	LevelTrace = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
	LevelBuss
)

const (
	Ltime  = 1 << iota //time format "2006/01/02 15:04:05"
	Lfile              //file.go:123
	Llevel             //[Trace|Debug|Info...]
)

var LogLevelString = map[string]int{
	"trace": LevelTrace,
	"debug": LevelDebug,
	"info":  LevelInfo,
	"warn":  LevelWarn,
	"error": LevelError,
	"fatal": LevelFatal,
	"buss":  LevelBuss,
}

var LevelName [7]string = [7]string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "BUSS"}

const TimeFormat = "2006/01/02 15:04:05"

const maxBufPoolSize = 16

type Logger struct {
	sync.Mutex

	level int
	flag  int

	handler Handler

	quit chan struct{}
	msg  chan []byte

	bufs [][]byte

	wg sync.WaitGroup

	closed bool
}

//new a logger with specified handler and flag
func New(handler Handler, flag int) *Logger {
	var l = new(Logger)

	l.level = LevelInfo
	l.handler = handler

	l.flag = flag

	l.quit = make(chan struct{})
	l.closed = false

	l.msg = make(chan []byte, 1024)

	l.bufs = make([][]byte, 0, 16)

	l.wg.Add(1)
	go l.run()

	return l
}

//new a default logger with specified handler and flag: Ltime|Lfile|Llevel
func NewDefault(handler Handler) *Logger {
	return New(handler, Ltime|Lfile|Llevel)
}

func newStdHandler() *StreamHandler {
	h, _ := NewStreamHandler(os.Stdout)
	return h
}

var std = NewDefault(newStdHandler())

type manager struct {
	mapper map[string]interface{}
	mu     sync.RWMutex
}

func newManager() *manager {
	m := new(manager)
	m.mapper = make(map[string]interface{})
	return m
}

func (self *manager) get(name string) *Logger {
	self.mu.Lock()
	defer self.mu.Unlock()

	l, ok := self.mapper[name]
	if ok {
		return l.(*Logger)
	} else {
		l = NewDefault(newStdHandler())
		self.mapper[name] = l
	}
	return l.(*Logger)
}

func (self *manager) close() {
	for _, v := range self.mapper {
		v.(*Logger).Close()
	}
}

var _mgr = newManager()

// like the python logging.getLogger
// return an Gloabl-logger and save in the memory
func GetLogger(name string) *Logger {
	if name == "" || name == "root" {
		return std
	}
	return _mgr.get(name)
}

func Close() {
	std.Close()
	_mgr.close()
}

func (l *Logger) run() {
	defer l.wg.Done()
	for {
		select {
		case msg := <-l.msg:
			l.handler.Write(msg)
			l.putBuf(msg)
		case <-l.quit:
			if len(l.msg) == 0 {
				return
			}
		}
	}
}

func (l *Logger) popBuf() []byte {
	l.Lock()
	var buf []byte
	if len(l.bufs) == 0 {
		buf = make([]byte, 0, 1024)
	} else {
		buf = l.bufs[len(l.bufs)-1]
		l.bufs = l.bufs[0 : len(l.bufs)-1]
	}
	l.Unlock()

	return buf
}

func (l *Logger) putBuf(buf []byte) {
	l.Lock()
	if len(l.bufs) < maxBufPoolSize {
		buf = buf[0:0]
		l.bufs = append(l.bufs, buf)
	}
	l.Unlock()
}

func (l *Logger) Close() {
	if l.closed {
		return
	}
	l.closed = true

	close(l.quit)
	l.wg.Wait()
	l.quit = nil

	l.handler.Close()
}

//set log level, any log level less than it will not log
func (l *Logger) SetLevel(level int) {
	l.level = level
}

func (l *Logger) Level() int {
	return l.level
}

func (l *Logger) SetHandler(h Handler) {
	l.handler = h
}

//a low interface, maybe you can use it for your special log format
//but it may be not exported later......
func (l *Logger) Output(callDepth int, level int, format string, v ...interface{}) {
	if l.level > level {
		return
	}

	buf := l.popBuf()

	if l.flag&Ltime > 0 {
		now := time.Now().Format(TimeFormat)
		buf = append(buf, now...)
		buf = append(buf, " - "...)
	}

	if l.flag&Llevel > 0 {
		buf = append(buf, LevelName[level]...)
		buf = append(buf, " - "...)
	}

	if l.flag&Lfile > 0 {
		_, file, line, ok := runtime.Caller(callDepth)
		if !ok {
			file = "???"
			line = 0
		} else {
			v := strings.Split(file, "/")
			idx := len(v) - 3
			if idx < 0 {
				idx = 0
			}
			file = strings.Join(v[idx:], "/")
		}

		buf = append(buf, file...)
		buf = append(buf, ":["...)

		buf = strconv.AppendInt(buf, int64(line), 10)
		buf = append(buf, "] - "...)
	}

	s := fmt.Sprintf(format, v...)

	buf = append(buf, s...)

	if s[len(s)-1] != '\n' {
		buf = append(buf, '\n')
	}

	l.msg <- buf
}

//log with Trace level
func (l *Logger) Trace(format string, v ...interface{}) {
	l.Output(2, LevelTrace, format, v...)
}

//log with Debug level
func (l *Logger) Debug(format string, v ...interface{}) {
	l.Output(2, LevelDebug, format, v...)
}

//log with info level
func (l *Logger) Info(format string, v ...interface{}) {
	l.Output(2, LevelInfo, format, v...)
}

//log with warn level
func (l *Logger) Warn(format string, v ...interface{}) {
	l.Output(2, LevelWarn, format, v...)
}

//log with error level
func (l *Logger) Error(format string, v ...interface{}) {
	l.Output(2, LevelError, format, v...)
}

//log with fatal level
func (l *Logger) Fatal(format string, v ...interface{}) {
	l.Output(2, LevelFatal, format, v...)
}

func (l *Logger) Buss(format string, v ...interface{}) {
	l.Output(2, LevelBuss, format, v...)
}

func SetLevel(level int) {
	std.SetLevel(level)
}

func SetLevelS(level string) {
	SetLevel(LogLevelString[strings.ToLower(level)])
}

func Trace(format string, v ...interface{}) {
	std.Output(2, LevelTrace, format, v...)
}

func Debug(format string, v ...interface{}) {
	std.Output(2, LevelDebug, format, v...)
}

func Info(format string, v ...interface{}) {
	std.Output(2, LevelInfo, format, v...)
}

func Warn(format string, v ...interface{}) {
	std.Output(2, LevelWarn, format, v...)
}

func Error(format string, v ...interface{}) {
	std.Output(2, LevelError, format, v...)
}

func Fatal(format string, v ...interface{}) {
	std.Output(2, LevelFatal, format, v...)
}

func Buss(format string, v ...interface{}) {
	std.Output(2, LevelBuss, format, v...)
}

func TraceE(format string, v ...interface{}) error {
	std.Output(2, LevelTrace, format, v...)
	return fmt.Errorf(format, v...)
}

func DebugE(format string, v ...interface{}) error {
	std.Output(2, LevelDebug, format, v...)
	return fmt.Errorf(format, v...)
}

func InfoE(format string, v ...interface{}) error {
	std.Output(2, LevelInfo, format, v...)
	return fmt.Errorf(format, v...)
}

func WarnE(format string, v ...interface{}) error {
	std.Output(2, LevelWarn, format, v...)
	return fmt.Errorf(format, v...)
}

func ErrorE(format string, v ...interface{}) error {
	std.Output(2, LevelError, format, v...)
	return fmt.Errorf(format, v...)
}

func FatalE(format string, v ...interface{}) error {
	std.Output(2, LevelFatal, format, v...)
	return fmt.Errorf(format, v...)
}

func TraceX(delta int, format string, v ...interface{}) {
	std.Output(2+delta, LevelTrace, format, v...)
}

func DebugX(delta int, format string, v ...interface{}) {
	std.Output(2+delta, LevelDebug, format, v...)
}

func InfoX(delta int, format string, v ...interface{}) {
	std.Output(2+delta, LevelInfo, format, v...)
}

func WarnX(delta int, format string, v ...interface{}) {
	std.Output(2+delta, LevelWarn, format, v...)
}

func ErrorX(delta int, format string, v ...interface{}) {
	std.Output(2+delta, LevelError, format, v...)
}

func FatalX(delta int, format string, v ...interface{}) {
	std.Output(2+delta, LevelFatal, format, v...)
}

func StdLogger() *Logger {
	return std
}

func GetLevel() int {
	return std.level
}
