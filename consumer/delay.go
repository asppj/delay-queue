package consumer

import (
	"context"
	"fmt"
	"time"
)

// 为什么最小到秒呢，因为最小误差很大，接近1s
const (
	second          = 1
	minute          = 60 * second
	hour            = 60 * minute
	sleepBaseSecond = 5 // 5s 不能太长，grateful stop sleep

	// 不少于5s的延迟
	delay5s  = 5 * second
	delay10s = 10 * second
	delay30s = 30 * second
	delay1m  = 1 * minute
	delay2m  = 2 * minute
	delay3m  = 3 * minute
	delay5m  = 5 * minute
	delay10m = 10 * minute
	delay15m = 15 * minute
	delay20m = 20 * minute
	delay30m = 30 * minute
	delay1h  = 1 * hour
	delay2h  = 2 * hour
	delay3h  = 3 * hour
	delay5h  = 5 * hour
	delay12h = 12 * hour
)

// exitNow true: 程序退出 false: 转发payload
// runNow true: 不需要暂定直接消费
func delayUntil(nowSecond, untilSecond int64) (runNow bool, err error) {
	if diff := untilSecond - nowSecond; diff > 0 {
		h, m, s := cutSecond(diff)
		fmt.Printf("sleep %dh:%dm:%ds\n", h, m, s)
		nb, ns := cutSecondBase(diff)
		if err = sleepDuration(nb, time.Second*sleepBaseSecond); err != nil {
			return false, err
		}
		if err = sleepDuration(ns, time.Second); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// true: 程序退出
// false: 休眠时间到
func sleepDuration(n int64, duration time.Duration) (err error) {
	for i := int64(0); i < n; i++ {
		if err = exitSleep(); err != nil {
			return fmt.Errorf("sleep (%d/%d/%ds) 提前退出：%v", i, n, duration/second, err)
		}
		time.Sleep(duration)
	}
	return nil
}

func cutSecond(ts int64) (int64, int64, int64) {
	h := ts / 3600
	m := (ts % 3600) / 60
	s := ts % 60
	return h, m, s
}

func cutSecondBase(ts int64) (nb int64, ns int64) {
	nb = ts / sleepBaseSecond
	ns = ts % sleepBaseSecond
	return
}

var (
	sleepCtx, sleepCancelFunc = context.WithCancel(context.Background())
)

// CancelDelaySleep 触发退出
func CancelDelaySleep() {
	sleepCancelFunc()
}

func exitSleep() error { // context cancel exit sleep
	return sleepCtx.Err()
}
