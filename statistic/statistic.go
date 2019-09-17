package statistic

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/qiniu/api.v7/auth"
	"github.com/qiniu/api.v7/conf"
)

const DefaultAPIHost = "api.qiniu.com"

// StatisticManager 数据统计接口
type StatisticManager struct {
	mac *auth.Credentials
}

// NewStatisticManager 用来构建一个新的 StatisticManager
func NewStatisticManager(mac *auth.Credentials) *StatisticManager {
	return &StatisticManager{mac: mac}
}

// SpaceReq 为获取标准存储的当前存储量的API请求内容
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
type SpaceReq struct {
	Bucket      string `json:"bucket"`
	BeginDate   string `json:"begin_date"`
	EndDate     string `json:"end_date"`
	Granularity string `json:"granularity"`
	Region      string `json:"region"`
}

// SpaceResp 为标准存储的当前存储量查询响应内容
type SpaceResp struct {
	Times []uint64 `json:"times,omitempty"`
	Datas []uint64 `json:"datas,omitempty"`
}

// GetSpace 方法用来获取标准存储的当前存储量
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
func (m *StatisticManager) GetSpace(req SpaceReq) (spaceData SpaceResp, err error) {
	path := fmt.Sprintf("/v6/space?bucket=%s&begin=%s&end=%s&g=%s",
		req.Bucket,
		req.BeginDate,
		req.EndDate,
		req.Granularity)
	resData, reqErr := getRequest(m.mac, path)
	if reqErr != nil {
		err = reqErr
		return
	}

	umErr := json.Unmarshal(resData, &spaceData)
	if umErr != nil {
		err = umErr
		return
	}
	return
}

// CountReq 为获取标准存储的当前文件数量的API请求内容
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
type CountReq struct {
	Bucket      string `json:"bucket"`
	BeginDate   string `json:"begin_date"`
	EndDate     string `json:"end_date"`
	Granularity string `json:"granularity"`
	Region      string `json:"region"`
}

// CountResp 为标准存储的当前文件数量查询响应内容
type CountResp struct {
	Times []uint64 `json:"times,omitempty"`
	Datas []uint64 `json:"datas,omitempty"`
}

// GetCount 方法用来获取标准存储的文件数量
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
func (m *StatisticManager) GetCount(req CountReq) (CountData CountResp, err error) {
	path := fmt.Sprintf("/v6/count?bucket=%s&begin=%s&end=%s&g=%s",
		req.Bucket,
		req.BeginDate,
		req.EndDate,
		req.Granularity)
	resData, reqErr := getRequest(m.mac, path)
	if reqErr != nil {
		err = reqErr
		return
	}

	umErr := json.Unmarshal(resData, &CountData)
	if umErr != nil {
		err = umErr
		return
	}
	return
}

// SpaceLineReq 为获取低频存储的当前存储量的API请求内容
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
// NoPredel 除去低频存储提前删除，剩余的存储量，值为1
// OnlyPredel 只显示低频存储提前删除的存储量，值为1
type SpaceLineReq struct {
	Bucket      string `json:"bucket"`
	BeginDate   string `json:"begin_date"`
	EndDate     string `json:"end_date"`
	Granularity string `json:"granularity"`
	Region      string `json:"region"`
	NoPredel    string `json:"no_predel"`
	OnlyPredel  string `json:"only_predel"`
}

// SpaceLineResp 为低频存储的当前存储量查询响应内容
type SpaceLineResp struct {
	Code  int      `json:"code"`
	Error string   `json:"error"`
	Times []uint64 `json:"times,omitempty"`
	Datas []uint64 `json:"datas,omitempty"`
}

// GetSpaceLine 方法用来获取低频存储的当前存储量
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
func (m *StatisticManager) GetSpaceLine(req SpaceLineReq) (spaceLineData SpaceLineResp, err error) {
	path := fmt.Sprintf("/v6/space_line?bucket=%s&region=%s&begin=%s&end=%s&g=%s",
		req.Bucket,
		req.Region,
		req.BeginDate,
		req.EndDate,
		req.Granularity)
	resData, reqErr := getRequest(m.mac, path)
	if reqErr != nil {
		err = reqErr
		return
	}

	umErr := json.Unmarshal(resData, &spaceLineData)
	if umErr != nil {
		err = umErr
		return
	}
	return
}

// CountLineReq 为获取低频存储的当前文件数量的API请求内容
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
type CountLineReq struct {
	Bucket      string `json:"bucket"`
	BeginDate   string `json:"begin_date"`
	EndDate     string `json:"end_date"`
	Granularity string `json:"granularity"`
	Region      string `json:"region"`
}

// CountLineResp 为低频存储的当前文件数量查询响应内容
type CountLineResp struct {
	Times []uint64 `json:"times,omitempty"`
	Datas []uint64 `json:"datas,omitempty"`
}

// GetCountLine 方法用来获取低频存储的当前文件数量
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
func (m *StatisticManager) GetCountLine(req CountLineReq) (CountLineData CountLineResp, err error) {
	path := fmt.Sprintf("/v6/count_line?bucket=%s&region=%s&begin=%s&end=%s&g=%s",
		req.Bucket,
		req.Region,
		req.BeginDate,
		req.EndDate,
		req.Granularity)
	resData, reqErr := getRequest(m.mac, path)
	if reqErr != nil {
		err = reqErr
		return
	}

	umErr := json.Unmarshal(resData, &CountLineData)
	if umErr != nil {
		err = umErr
		return
	}
	return
}

// BlobTransferReq 为获取当前跨区域同步流量的API请求内容
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
// SelectType 值为size，表示存储量 (Byte)
// IsOversea 是否为海外同步 0 国内 1 海外
// TaskID 任务 id
type BlobTransferReq struct {
	BeginDate   string `json:"begin_date"`
	EndDate     string `json:"end_date"`
	Granularity string `json:"granularity"`
	SelectType  string `json:"select_type"`
	IsOversea   string `json:"is_oversea"`
	TaskID      string `json:"task_id"`
}

// BlobTransferResp 为当前跨区域同步流量查询响应内容
type BlobTransferResp []BlobTransferData

// 为当前跨区域同步流量查询响应内容
type BlobTransferData struct {
	Time   string `json:"time"`
	Values struct {
		Size uint64 `json:"size"`
	} `json:"values"`
}

// GetBlobTransfer 方法用来获取跨区域同步流量
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
func (m *StatisticManager) GetBlobTransfer(req BlobTransferReq) (blobTransferData BlobTransferResp, err error) {
	path := fmt.Sprintf("/v6/blob_transfer?begin=%s&end=%s&g=%s&select=%s&$is_oversea=%s&$taskid=%s",
		req.BeginDate,
		req.EndDate,
		req.Granularity,
		req.SelectType,
		req.IsOversea,
		req.TaskID)
	resData, reqErr := getRequest(m.mac, path)
	if reqErr != nil {
		err = reqErr
		return
	}

	umErr := json.Unmarshal(resData, &blobTransferData)
	if umErr != nil {
		err = umErr
		return
	}
	return
}

// RSChTypeReq 为获取当前存储类型转换请求次数的API请求内容
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
// SelectType 值为size，表示存储量 (Byte)
// Bucket 空间名称
// Region 存储区域
type RSChTypeReq struct {
	BeginDate   string `json:"begin_date"`
	EndDate     string `json:"end_date"`
	Granularity string `json:"granularity"`
	SelectType  string `json:"select_type"`
	Bucket      string `json:"bucket"`
	Region      string `json:"region"`
}

// RSChTypeResp 为当前存储类型转换请求次数查询响应内容
type RSChTypeResp []RSChTypeData

// 为当前存储类型转换请求次数查询响应内容
type RSChTypeData struct {
	Time   string `json:"time"`
	Values struct {
		Hits uint64 `json:"hits"`
	} `json:"values"`
}

// GetRSChType 方法用来获取存储类型转换请求次数
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
func (m *StatisticManager) GetRSChType(req RSChTypeReq) (rSChTypeResp RSChTypeResp, err error) {
	path := fmt.Sprintf("/v6/rs_chtype?begin=%s&end=%s&g=%s&select=%s&$bucket=%s&$region=%s",
		req.BeginDate,
		req.EndDate,
		req.Granularity,
		req.SelectType,
		req.Bucket,
		req.Region)
	resData, reqErr := getRequest(m.mac, path)
	if reqErr != nil {
		err = reqErr
		return
	}

	umErr := json.Unmarshal(resData, &rSChTypeResp)
	if umErr != nil {
		err = umErr
		return
	}
	return
}

// BlobIOReq 为获取当前外网流出流量统计和 GET 请求次数的API请求内容
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
// SelectType flow 外网流出流量 (Byte) hits GET 请求次数
// Bucket 空间名称
// FType 存储类型 0 标准存储 1 低频存储
// Domain 空间访问域名
// Region 存储区域
// Src 请求涞源 origin 用户直接到源站的请求 inner 专线或内网请求 ex 专线到源站的下载请求 atlab 七牛数据处理的下载请求
type BlobIOReq struct {
	BeginDate   string `json:"begin_date"`
	EndDate     string `json:"end_date"`
	Granularity string `json:"granularity"`
	SelectType  string `json:"select_type"`
	Bucket      string `json:"bucket"`
	FType       string `json:"ftype"`
	Domain      string `json:"domain"`
	Region      string `json:"region"`
	Src         string `json:"src"`
}

// BlobIOReq 为当前外网流出流量统计和 GET 请求次数查询响应内容
type BlobIOResp []BlobIOData

// 为当前外网流出流量统计和 GET 请求次数查询响应内容
type BlobIOData struct {
	Time   string `json:"time"`
	Values struct {
		Hits uint64 `json:"hits"`
		Flow uint64 `json:"flow"`
	} `json:"values"`
}

// GetBlobIO 方法用来获取外网流出流量统计和 GET 请求次数
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
func (m *StatisticManager) GetBlobIO(req BlobIOReq) (blobIOResp BlobIOResp, err error) {
	path := fmt.Sprintf("/v6/blob_io?begin=%s&end=%s&g=%s&select=%s&$bucket=%s&$domain=%s&$region=%s&$src=%s",
		req.BeginDate,
		req.EndDate,
		req.Granularity,
		req.SelectType,
		req.Bucket,
		req.Domain,
		req.Region,
		req.Src,
	)
	resData, reqErr := getRequest(m.mac, path)
	if reqErr != nil {
		err = reqErr
		return
	}

	umErr := json.Unmarshal(resData, &blobIOResp)
	if umErr != nil {
		err = umErr
		return
	}

	return
}

// RsPutReq 为获取当前外网流出流量统计和 GET 请求次数的API请求内容
// BeginDate 	开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
// SelectType flow 外网流出流量 (Byte) hits GET 请求次数
// Bucket 空间名称
// FType 存储类型 0 标准存储 1 低频存储
// Region 存储区域
type RsPutReq struct {
	BeginDate   string `json:"begin_date"`
	EndDate     string `json:"end_date"`
	Granularity string `json:"granularity"`
	SelectType  string `json:"select_type"`
	Bucket      string `json:"bucket"`
	FType       string `json:"ftype"`
	Region      string `json:"region"`
}

// RsPutResp 为当前外网流出流量统计和 GET 请求次数查询响应内容
type RsPutResp []RsPutData

// 为当前外网流出流量统计和 GET 请求次数查询响应内容
type RsPutData struct {
	Time   string `json:"time"`
	Values struct {
		Hits uint64 `json:"hits"`
	} `json:"values"`
}

// GetRsPut 方法用来获取外网流出流量统计和 GET 请求次数
// Bucket 存储空间名称，是一个条件请求参数
// BeginDate 开始日期，格式例如：20060102150405
// EndDate 	结束日期，格式例如：20060102150405
// Granularity	取值粒度，取值可选值：5min/hour/day
func (m *StatisticManager) GetRsPut(req RsPutReq) (rsPutResp RsPutResp, err error) {
	path := fmt.Sprintf("/v6/rs_put?begin=%s&end=%s&g=%s&select=%s&$bucket=%s&$region=%s",
		req.BeginDate,
		req.EndDate,
		req.Granularity,
		req.SelectType,
		req.Bucket,
		req.Region,
	)
	resData, reqErr := getRequest(m.mac, path)
	if reqErr != nil {
		err = reqErr
		return
	}

	umErr := json.Unmarshal(resData, &rsPutResp)
	if umErr != nil {
		err = umErr
		return
	}
	return
}

func getRequest(mac *auth.Credentials, path string) (resData []byte,
	err error) {
	urlStr := fmt.Sprintf("https://%s%s", DefaultAPIHost, path)
	req, reqErr := http.NewRequest("POST", urlStr, nil)
	if reqErr != nil {
		err = reqErr
		return
	}

	accessToken, signErr := mac.SignRequest(req)
	if signErr != nil {
		err = signErr
		return
	}

	req.Header.Add("Authorization", "QBox "+accessToken)
	req.Header.Add("Content-Type", conf.CONTENT_TYPE_JSON)

	resp, respErr := http.DefaultClient.Do(req)
	if respErr != nil {
		err = respErr
		return
	}
	defer resp.Body.Close()

	resData, ioErr := ioutil.ReadAll(resp.Body)
	if ioErr != nil {
		err = ioErr
		return
	}

	return
}
