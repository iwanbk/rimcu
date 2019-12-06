package rimcu

import (
	"github.com/gomodule/redigo/redis"
)

type resp2NotifSubcriber struct {
	pool              *redis.Pool
	finishedCh        chan struct{}
	logger            Logger
	disconnectHandler func()
	notifHandler      func([]byte)
	channel           string
}

func newResp2NotifSubcriber(pool *redis.Pool, notifHandler func([]byte), disconnectHandler func(), channel string) *resp2NotifSubcriber {
	return &resp2NotifSubcriber{
		pool:              pool,
		finishedCh:        make(chan struct{}),
		logger:            &debugLogger{},
		notifHandler:      notifHandler,
		disconnectHandler: disconnectHandler,
		channel:           channel,
	}
}

// TODO: call it
func (lc *resp2NotifSubcriber) Close() {
	lc.finishedCh <- struct{}{}
}
func (lc *resp2NotifSubcriber) runSubscriber() error {
	subscriberDoneCh, err := lc.startSub()
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-lc.finishedCh: // we are done
				return
			case <-subscriberDoneCh:
				// we're just disconnected from our Notif channel,
				// clear our in mem cache as we can't assume that the values
				// still updated
				lc.disconnectHandler()

				// start new subscriber
				subscriberDoneCh, err = lc.startSub()
				if err != nil {
					lc.logger.Errorf("failed to start subscriber: %v", err)
				}
			}
		}
	}()
	return nil
}

// starts subscriber to listen to all of synchronization message sent by other nodes
func (lc *resp2NotifSubcriber) startSub() (chan struct{}, error) {
	doneCh := make(chan struct{})

	// setup subscriber
	sub, err := lc.subscribe(lc.channel)
	if err != nil {
		close(doneCh)
		return doneCh, err
	}

	// we're just connected to our Notif channel,
	// it means we previously not connected or disconnected from the Notif channel.
	lc.disconnectHandler()

	// run subscriber loop
	go func() {
		defer func() {
			close(doneCh)
			sub.Close()
		}()

		for {
			switch v := sub.Receive().(type) {
			case redis.Message:
				lc.notifHandler(v.Data)
			case error:
				// TODO: don't log if it is not error, i.e.: on cache Close
				lc.logger.Errorf("subscribe err: %v", v)
				return
			}
		}
	}()
	return doneCh, nil
}

// subscribe to the notification channel
func (lc *resp2NotifSubcriber) subscribe(channel string) (*redis.PubSubConn, error) {
	conn := lc.pool.Get()
	if err := conn.Err(); err != nil {
		return nil, err
	}
	sub := &redis.PubSubConn{Conn: conn}

	err := sub.Subscribe(channel)
	if err != nil {
		sub.Close()
		return nil, err
	}
	return sub, nil
}
