package consumer

import (
	"encoding/json"
	"time"
)

// PayloadDelay 延迟队列数据
type PayloadDelay struct {
	PushUnix int64  `json:"pushUnix"` // 秒
	PushTime string `json:"pushTime"` // 2021-3-11 16:14:17
	Payload  string `json:"payload"`  //
	Topic    string `json:"topic"`    // 到期之后放入的队列
}

func loadPayload(payload []byte) (res PayloadDelay, err error) {
	err = json.Unmarshal(payload, &res)
	return
}

// NewDefaultDelayPayload 默认
func NewDefaultDelayPayload(payload string, topic string) PayloadDelay {
	return PayloadDelay{
		PushUnix: time.Now().Unix(),
		PushTime: time.Now().Format("2006-01-02 15:04:05"),
		Payload:  payload,
		Topic:    topic,
	}
}

func newPushUnix() int64 {
	n := time.Now().Unix()
	n = n - n%(60*5)
	return n
}

// err graceful退出
// runNow true:不需要暂定直接消费
func (p PayloadDelay) delay(duration int64) (runNow bool, err error) {
	nowSecond := time.Now().Unix()
	untilSecond := p.PushUnix + duration
	return delayUntil(nowSecond, untilSecond)
}

// 投递到对应队列
func (p PayloadDelay) deliver() (ok bool, err error) {
	// TODO 转发
	return false, nil
}
