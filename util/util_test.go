package util

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/fredgan/go-utils/uuid"
)

func TestMap(t *testing.T) {

	r := Map(func(a interface{}) interface{} {
		s := a.(string)
		return s + "a"

	}, "a", "b", "c")
	if len(r) != 3 || r[0] != "aa" || r[1] != "ba" || r[2] != "ca" {
		t.Fatal("invalid result ", r)
	}
}

type Y struct {
}

type X struct {
	*Y
	X int `json:"x,string"`
}

func TestStruct2map(t *testing.T) {
	var x X
	m := Struct2Map(&x)
	if len(m) != 1 && m["x"] != 0 {
		t.Errorf("invalid result %#v", m)
	}
}

func TestUtil(t *testing.T) {
	// test Remove
	ss := []string{"ccc", "a", "b", "ccc", "abc"}
	n := Remove(&ss, "ccc")
	if n != 2 || len(ss) != 3 {
		t.Fatal("Remove failed ", n, ss)
	}

	nn := []int{10, 10, 10}
	n = Remove(&nn, 10)
	if n != 3 || len(nn) != 0 {
		t.Fatal("Remove failed", n, nn)
	}

	// test Find
	findArray := []string{"aa", "bb", "cc", "bb"}
	if i := Find(findArray, "bb"); i != 1 {
		t.Fatal("Find failed", i)
	}

	findArray2 := []int{10, 20, 30, 40}
	if i := Find(findArray2, 25); i != -1 {
		t.Fatal("Find failed", i)
	}
}

func TestAssign(t *testing.T) {
	var val interface{}
	val = int64(1200)

	var e int64
	Assign(&e, val)
	if e != 1200 {
		t.Fatal("assign failed", e)
	}

	func() {
		defer func() {
			r := recover()
			fmt.Println(r, "that's ok")
		}()

		var ee int
		Assign(&ee, val)

		t.Fatal("assign to wrong type")
	}()
}

func TestGetRealIP(t *testing.T) {
	var req *http.Request
	SpIpInit()
	req = new(http.Request)
	req.Header = make(map[string][]string, 0)
	req.Header.Set("X-Forwarded-For", "   192.168.0.3  , 113.106.106.3, 127.0.0.1")
	realip := GetRealIP(req)
	if realip != "113.106.106.3" {
		t.Errorf("find real ip %s", realip)
	}
}

var uas = []string{
	// firefox
	"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0",

	// qqbrowser
	"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/9.0.3100.400)",
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.69 Safari/537.36 QQBrowser/9.0.3100.400",

	// the world
	"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E;  TheWorld 6)",
	"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E;  TheWorld 6)",

	// maxthon
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.7.1000 Chrome/30.0.1599.101 Safari/537.36",

	// uc
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.118 UBrowser/5.2.3635.32 Safari/537.36",

	// liebao
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36 LBBROWSER",
	"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
	"Mozilla/4.0 (compatible; MSIE 7.0; LBBROWSER)",

	// sougou
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36 SE 2.X MetaSr 1.0",

	// opera
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.107 Safari/537.36 OPR/31.0.1889.99",

	// chrome
	"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.155 Safari/537.36",
	"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.155 Safari/537.36",

	// ie
	"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",

	// safari
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",

	// taobao
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.5 Safari/536.11",

	// chrome
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.130 Safari/537.36",

	// chrome
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.130 Safari/537.36",

	// safari
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/600.6.3 (KHTML, like Gecko) Version/8.0.6 Safari/600.6.3",

	// safari
	"Mozilla/5.0 (iPod touch; CPU iPhone OS 8_4 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12H143 Safari/600.1.4",

	// iphone safari
	"Mozilla/5.0 (iPhone; CPU iPhone OS 8_1_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12B466 Safari/600.1.4",

	// uc
	"Mozilla/5.0 (iPod touch; CPU iPhone OS 8_4 like Mac OS X; zh-CN) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/12H143 UCBrowser/10.5.5.611 Mobile",

	// xiaomi
	"Mozilla/5.0 (Linux; U; Android 4.4.4; zh-cn; MI 4LTE Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/42.0.0.0 Mobile Safari/537.36 XiaoMi/MiuiBrowser/2.1.1",

	// firefox
	"Mozilla/5.0 (X11; FreeBSD amd64; rv:28.0) Gecko/20100101 Firefox/28.0",

	// firefox
	"Mozilla/5.0 (X11; U; SunOS sun4u; en-US; rv:1.8.1.11) Gecko/20080118 Firefox/2.0.0.11",

	// edge
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.10240",

	// Tencent Traveler
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/4.0; TencentTraveler 4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",

	// 2345
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; 2345Explorer/6.1.0.8631)",

	// 2345
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.99 Safari/537.36 2345Explorer/6.1.0.8631",
}

func TestGetRealBrowser(t *testing.T) {
	// res := []string{"firefox 40.0", "qq 9.0.3100.400", "qq 9.0.3100.400", "theworld 6", "theworld 6", "maxthon 4.4.7.1000", "uc 5.2.3635.32", "liebao", "liebao", "liebao",
	// 	"sogou 1.0", "opera 31.0.1889.99", "chrome 44.0.2403.155", "chrome 44.0.2403.155", "ie 8.0", "safari 5.1.7", "taobao 3.5", "chrome 44.0.2403.130",
	// 	"chrome 44.0.2403.130", "safari 8.0.6", "safari 8.0", "safari 8.0", "uc 10.5.5.611", "miui 2.1.1", "firefox 28.0", "firefox 2.0.0.11",
	// 	"edge 12.10240", "qq 4.0", "2345 6.1.0.8631", "2345 6.1.0.8631"}
	res := []string{"firefox",
		"qq",
		"qq",
		"theworld",
		"theworld",
		"maxthon",
		"uc",
		"liebao",
		"liebao",
		"liebao",
		"sogou",
		"opera",
		"chrome",
		"chrome",
		"ie",
		"safari",
		"taobao",
		"chrome",
		"chrome",
		"safari",
		"safari",
		"safari",
		"uc",
		"miui",
		"firefox",
		"firefox",
		"edge",
		"qq",
		"2345",
		"2345"}

	for i, ua := range uas {
		browser := GetRealBrowser(ua)
		if browser != res[i] {
			t.Errorf("Browser not match. [index=%d, assertBrowser=%s, realBrowser=%s]", i, res[i], browser)
		}
	}
}

func TestGetRealPlatform(t *testing.T) {
	res := []string{"Windows 7", "Windows 7", "Windows 7", "Windows 7", "Windows 7", "Windows 7", "Windows 7", "Windows 7", "Windows 7", "Windows", "Windows 7",
		"Windows 7", "Windows 7", "Windows 7", "Windows 7", "Windows 7", "Windows 7", "Linux x86_64", "Mac OS X 10.10.3", "Mac OS X 10.10.3", "iOS 8.4", "iOS 8.1.3",
		"iOS 8.4", "Android 4.4.4", "Unix", "Unix", "Windows 10", "Windows 7", "Windows 7", "Windows 7"}

	for i, ua := range uas {
		platform := GetRealPlatform(ua)
		if platform != res[i] {
			t.Errorf("Platform not match. [index=%d, assertPlatform=%s, realPlatform=%s]", i, res[i], platform)
		}
	}
}

func TestGetDevice(t *testing.T) {
	req := &http.Request{}

	deviceId := uuid.NewUUID().String()

	req.Header = http.Header{}
	req.Header.Set("Device-Id", deviceId)
	req.Header.Set("Device-Name", "devicename")
	req.Header.Set("Device-Platform", "Linux")
	req.Header.Set("X-Client-Ver", "1.1.1")

	req.URL = &url.URL{}

	device := GetDevice(req, nil)
	if device.Id != deviceId {
		t.Errorf("Device id not match. [assertId=%s, realId=%s]", deviceId, device.Id)
	}

	fmt.Printf("%v\n", device)
}
