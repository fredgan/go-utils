package model

type Device struct {
	Id         string `json:"deviceid"`
	Name       string `json:"name"`
	Platform   string `json:"platform"`
	AppVersion string `json:"client_ver"`
	Type       string `json:"dev_type"`
	LastTime   int64  `json:"last_time"`
	IP         string `json:"ip"`
}

type DeviceSorter struct {
	Devices []*Device
	SortBy  func(d1, d2 *Device) bool
}

func (s *DeviceSorter) Len() int {
	return len(s.Devices)
}

func (s *DeviceSorter) Swap(i, j int) {
	s.Devices[i], s.Devices[j] = s.Devices[j], s.Devices[i]
}

func (s *DeviceSorter) Less(i, j int) bool {
	return s.SortBy(s.Devices[i], s.Devices[j])
}

const (
	BrowserDeviceId     = "E31203124E11465A93E8156147D7F9F1"
	BrowserDeviceName   = "浏览器" //TODO 中文翻译
	BrowserWechat       = "微信"
	BrowserLiebao       = "猎豹"
	BrowserTheWorld     = "世界之窗"
	BrowserBaidu        = "百度"
	BrowserMaxthon      = "傲游"
	BrowserSogou        = "搜狗"
	BrowserMiui         = "小米"
	BrowserTaobao       = "淘宝"
	BrowserDeviceClient = "WEB_BROWSER_QING"
	PCClient            = "PC客户端"
)

const (
	DeviceTypeBrowser      = "web"
	DeviceTypePC           = "pc"
	DeviceTypeAndroid      = "android"
	DeviceTypeIOS          = "ios"
	DeviceTypeWindowsPhone = "windowsphone"
	DeviceTypeUnkown       = "unknown"
)