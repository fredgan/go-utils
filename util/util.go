package util

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/mssola/user_agent"
	"github.com/fredgan/go-utils/model"

	"github.com/fredgan/go-utils/log"
)

// Assign val to the address of ptr. And the type of val must be *ptr.
// You can think that it works like this:
//   v, ok := val.(type(*ptr))
//   if !ok { panic() }
//   *ptr = v
func Assign(ptr interface{}, val interface{}) {
	pv := reflect.ValueOf(ptr)
	if pv.Kind() != reflect.Ptr {
		panic("ptr must be a pointer")
	}
	ev := pv.Elem()
	if !ev.CanSet() {
		panic("elem of ptr can not be set")
	}

	vv := reflect.ValueOf(val)

	if vv.Type() != ev.Type() {
		panic(fmt.Sprintf("type not match (%v, %v)", vv.Type().String(), ev.Type().String()))
	}

	ev.Set(vv)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
//coding by Shang Peng
//使用GetRealIP前需要调用SpIpInit初始化内网IP库
var LocalIPs = []string{"127.0.0.1/32"}
var IntranetIPs = append(LocalIPs, "10.0.0.0/8", "172.31.0.0/16", "172.16.0.0/16", "192.168.0.0/16")
var SpecialIPs = make([]*net.IPNet, 0, len(IntranetIPs)) //内网IP库

func GetRealIP(r *http.Request) string {
	if r == nil {
		return ""
	}

	realIP := r.Header.Get("X-Forwarded-For")
	//fmt.Println(realIP)
	if realIP != "" {
		ipList := strings.Split(realIP, ",")
		for _, proxyip := range ipList {
			if isSpecialIP(strings.TrimSpace(proxyip)) == false {
				return strings.TrimSpace(proxyip)
			}
		}

		return strings.TrimSpace(ipList[0])
	}

	realIP = r.Header.Get("X-Real-IP")
	if realIP != "" {
		return realIP
	}

	return strings.SplitN(r.RemoteAddr, ":", 2)[0]
}

func isSpecialIP(addr string) bool {
	ip := net.ParseIP(addr)
	if ip == nil {
		log.Warn("net.ParseIP error")
		return false // parse error
	}
	for _, ipnet := range SpecialIPs {
		if ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

func SpIpInit() {
	for _, s := range IntranetIPs {
		if !strings.Contains(s, "/") {
			s += "/32"
		}
		_, ipnet, err := net.ParseCIDR(s)
		if err != nil {
			panic("invalid IP filter: " + s)
		}
		SpecialIPs = append(SpecialIPs, ipnet)
	}
}

//GetRealIP end
//////////////////////////////////////////////////////////////////////////////////////////////////////////////

var emailPattern = regexp.MustCompile("^(\\w[\\-\\.\\+]*?)*\\w@(\\w[\\-\\._]*?)*\\w\\.\\w{2,10}$")

func IsEmail(email string) bool {
	return emailPattern.Match([]byte(email))
}

func GetMd5(raw []byte) string {
	h := md5.New()
	h.Write(raw)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func GetBase64Md5(raw []byte) string {
	h := md5.New()
	h.Write(raw)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func Map(mapping func(interface{}) interface{}, strs ...interface{}) []interface{} {
	for i, str := range strs {
		strs[i] = mapping(str)
	}
	return strs
}

// return base name and ext with leading "."
func SplitFilename(fname string) (base string, ext string) {
	ext = path.Ext(fname)
	base = strings.TrimSuffix(fname, ext)
	return base, ext
}

func NowMsec() int64 {
	//返回当前毫秒时间戳
	return time.Now().UnixNano() / 1e6
}

var Browsers = []string{"chrome", "firefox", "safari", "opera", "edge", "ie", "wx", "qq", "theworld", "liebao", "baidu", "maxthon", "sogou", "miui", "uc", "360", "taobao", "2345"}
var Platforms = []string{"Linux", "Unix", "Mac OS X", "Windows", "iOS", "Android", "Windows Phone"}

func IsCommonBrowser(browser string) bool {
	for _, commonBrowser := range Browsers {
		if commonBrowser == browser {
			return true
		}
	}
	return false
}

func IsCommonPlatform(platform string) bool {
	for _, commonPlatform := range Platforms {
		if strings.Contains(platform, commonPlatform) {
			return true
		}
	}
	return false
}

func GetVersion(ua string, keyword string) string {
	regStr := fmt.Sprintf("%s\\d+(\\.\\d+)*", keyword)
	version := regexp.MustCompile(regStr).FindString(ua)
	return version
}

func GetRealBrowser(ua string) string {
	userAgent := user_agent.New(ua)
	browser, _ := userAgent.Browser()
	browser = strings.ToLower(browser)
	if browser == "internet explorer" {
		browser = "ie"
	}

	if IsCommonBrowser(browser) && browser != "chrome" && browser != "ie" && browser != "safari" {
		return browser
	}

	lowerUa := strings.ToLower(ua)

	if strings.Contains(lowerUa, "chromium") {
		browser = "chrome"
	} else if strings.Contains(lowerUa, "micromessenger") {
		browser = "wx"
	} else if strings.Contains(lowerUa, "qq") || strings.Contains(lowerUa, "tencent") {
		browser = "qq"
	} else if strings.Contains(lowerUa, "theworld") {
		browser = "theworld"
	} else if strings.Contains(lowerUa, "lbbrowser") {
		browser = "liebao"
	} else if strings.Contains(lowerUa, "maxthon") {
		browser = "maxthon"
	} else if strings.Contains(lowerUa, "metasr") || strings.Contains(lowerUa, "sogou") {
		browser = "sogou"
	} else if strings.Contains(lowerUa, "miui") {
		browser = "miui"
	} else if strings.Contains(lowerUa, "bidubrowser") {
		browser = "baidu"
	} else if strings.Contains(lowerUa, "ucweb") || strings.Contains(lowerUa, "ucbrowser") || strings.Contains(lowerUa, "ubrowser") {
		browser = "uc"
	} else if strings.Contains(lowerUa, "360") {
		browser = "360"
	} else if strings.Contains(lowerUa, "taobrowser") {
		browser = "taobao"
	} else if strings.Contains(lowerUa, "edge") {
		browser = "edge"
	} else if strings.Contains(lowerUa, "2345") {
		browser = "2345"
	}

	return browser
}

func GetRealPlatform(ua string) string {
	userAgent := user_agent.New(ua)
	platform := userAgent.OS()

	if strings.Contains(ua, "Windows NT 10") {
		platform = "Windows 10"
	}

	if strings.Contains(platform, "Linux") || strings.Contains(platform, "Windows") ||
		strings.Contains(platform, "Android") {
		return platform
	}

	if strings.Contains(ua, "Mac OS X") && !strings.Contains(platform, "Mac OS X") {
		reg := regexp.MustCompile("CPU iPhone OS \\d_\\d like Mac OS X")
		platform = reg.FindString(ua)
	}

	if strings.Contains(platform, "Mac OS X") {
		if strings.Contains(platform, "Intel") {
			platform = strings.Replace(platform, "Intel", "", -1)
			platform = strings.Replace(platform, "_", ".", -1)
			return strings.TrimSpace(platform)
		}

		platform = strings.Replace(platform, "CPU iPhone OS", "iOS", -1)
		platform = strings.Replace(platform, "like Mac OS X", "", -1)
		platform = strings.Replace(platform, "_", ".", -1)
		return strings.TrimSpace(platform)
	}

	lowerUa := strings.ToLower(ua)
	if !IsCommonPlatform(platform) {
		if strings.Contains(lowerUa, "windows nt 10.0") {
			platform = "Windows 10"
		} else if strings.Contains(lowerUa, "windows nt 6.3") {
			platform = "Windows 8.1"
		} else if strings.Contains(lowerUa, "windows nt 6.2") {
			platform = "Windows 8"
		} else if strings.Contains(lowerUa, "windows nt 6.1") {
			platform = "Windows 7"
		} else if strings.Contains(lowerUa, "windows nt 6.0") {
			platform = "Windows Vista"
		} else if strings.Contains(lowerUa, "windows nt 5") {
			platform = "Windows XP"
		} else if strings.Contains(lowerUa, "msie") {
			platform = "Windows"
		} else if strings.Contains(lowerUa, "bsd") || strings.Contains(lowerUa, "sunos") || strings.Contains(lowerUa, "solaris") {
			platform = "Unix"
		} else if strings.Contains(lowerUa, "x11") {
			platform = "Linux"
		} else if strings.Contains(lowerUa, "iphone") {
			platform = "iOS"
		}
	}

	return platform
}

func ToExportBrowser(browser string) string {
	if strings.Index(browser, "wx") == 0 {
		return strings.Replace(browser, "wx", model.BrowserWechat, 1)
	}

	if strings.Index(browser, "liebao") == 0 {
		browser = strings.Replace(browser, "liebao", model.BrowserLiebao, 1)
	} else if strings.Index(browser, "theworld") == 0 {
		browser = strings.Replace(browser, "theworld", model.BrowserTheWorld, 1)
	} else if strings.Index(browser, "baidu") == 0 {
		browser = strings.Replace(browser, "baidu", model.BrowserBaidu, 1)
	} else if strings.Index(browser, "maxthon") == 0 {
		browser = strings.Replace(browser, "maxthon", model.BrowserMaxthon, 1)
	} else if strings.Index(browser, "sogou") == 0 {
		browser = strings.Replace(browser, "sogou", model.BrowserSogou, 1)
	} else if strings.Index(browser, "miui") == 0 {
		browser = strings.Replace(browser, "miui", model.BrowserMiui, 1)
	} else if strings.Index(browser, "taobao") == 0 {
		browser = strings.Replace(browser, "taobao", model.BrowserTaobao, 1)
	}

	return fmt.Sprintf("%s%s", browser, model.BrowserDeviceName)
}

func GetDeviceType(req *http.Request, body url.Values) string {
	var values url.Values
	if body != nil {
		values = body
	} else {
		values = req.URL.Query()
	}

	platform := req.Header.Get("Device-Platform")
	if platform == "" {
		platform = values.Get("platform")
	}

	if strings.Contains(platform, "Android") {
		return model.DeviceTypeAndroid
	}

	if strings.Contains(platform, "iOS") {
		return model.DeviceTypeIOS
	}

	if strings.Contains(platform, "Windows Phone") {
		return model.DeviceTypeWindowsPhone
	}

	if strings.Contains(platform, "Linux") || strings.Contains(platform, "Windows") {
		return model.DeviceTypePC
	}

	appversion := req.Header.Get("X-Client-Ver")
	if appversion == "" {
		appversion = values.Get("appversion")
	}

	lowerVersion := strings.ToLower(appversion)
	if strings.Index(lowerVersion, "ios") == 0 {
		return model.DeviceTypeIOS
	} else if strings.Index(lowerVersion, "android") == 0 {
		return model.DeviceTypeAndroid
	}

	if values.Get("isiosapp") == "true" {
		return model.DeviceTypeIOS
	}

	if values.Get("isandroidapp") == "true" {
		return model.DeviceTypeAndroid
	}

	deviceId := req.Header.Get("Device-ID")
	if deviceId == "" {
		deviceId = values.Get("deviceid")
	}

	if deviceId == "" {
		ua := req.Header.Get("User-Agent")
		browser := GetRealBrowser(ua)
		lowerUa := strings.ToLower(ua)

		if strings.Contains(lowerUa, "qing") || strings.Contains(lowerUa, "qtwebkit") {
			return model.DeviceTypePC
		}

		if IsCommonBrowser(strings.Split(browser, " ")[0]) || strings.Contains(lowerUa, "browser") {
			return model.DeviceTypeBrowser
		}
	}

	return model.DeviceTypeUnkown
}

func GetDevice(req *http.Request, body url.Values) *model.Device {
	var values url.Values
	if body != nil {
		values = body
	} else {
		values = req.URL.Query()
	}
	log.Debug("header%v, values %v", req.Header, values)

	deviceId := req.Header.Get("Device-ID")
	if deviceId == "" {
		deviceId = values.Get("deviceid")
	}

	deviceName := req.Header.Get("Device-Name")
	if deviceName == "" {
		deviceName = values.Get("devicename")
	}

	appversion := req.Header.Get("X-Client-Ver")
	if appversion == "" {
		appversion = values.Get("appversion")
	}

	platform := req.Header.Get("Device-Platform")
	if platform == "" {
		platform = values.Get("platform")
	}
	if platform == "" {
		lowerVersion := strings.ToLower(appversion)
		if strings.Index(lowerVersion, "ios") == 0 {
			platform = "iOS"
		} else if strings.Index(lowerVersion, "android") == 0 {
			platform = "Android"
		}
	}
	if platform == "" {
		if values.Get("isiosapp") == "true" {
			platform = "iOS"
		}

		if values.Get("isandroidapp") == "true" {
			platform = "Android"
		}
	}

	if deviceId == "" {
		ua := req.Header.Get("User-Agent")

		deviceId = GetMd5([]byte(ua))
		deviceName = GetRealBrowser(ua)
		platform = GetRealPlatform(ua)
	}

	return &model.Device{
		Id:         deviceId,
		Name:       deviceName,
		Platform:   platform,
		AppVersion: appversion,
		Type:       GetDeviceType(req, body),
		IP:         GetRealIP(req),
		LastTime:   time.Now().Unix(),
	}
}

func Int2Bool(i int64) bool {
	return i != 0
}

func Bool2Int(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func CharCount(s string) int {
	return len([]rune(s))
}

var phoneRegionMapping = map[string]*regexp.Regexp{
	"0086": regexp.MustCompile("^(0086|086|86)?(1[3-9]\\d{9})$"),
}

func IsValidCellPhone(phonenumber string) bool {
	region, phone := ParseCellPhone(phonenumber)
	if region == "" && phone == "" {
		return false
	}
	return true
}

func ParseCellPhone(phonenumber string) (region string, phone string) {
	for region, r := range phoneRegionMapping {
		matchs := r.FindStringSubmatch(phonenumber)
		if len(matchs) != 0 {
			return region, matchs[2]
		}
	}
	return "", ""
}
