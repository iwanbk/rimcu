package resp2

import (
	"fmt"

	"github.com/iwanbk/rimcu/internal/redigo/redis"
	logger "github.com/iwanbk/rimcu/logger"
)

type notifSubcriber struct {
	pool              *redis.Pool
	finishedCh        chan struct{}
	logger            logger.Logger
	disconnectHandler func()
	notifHandler      func(string)
	clientID          int64
}

func newNotifSubcriber(pool *redis.Pool, notifHandler func(string), disconnectHandler func(),
	logger logger.Logger) *notifSubcriber {
	ns := &notifSubcriber{
		pool:              pool,
		finishedCh:        make(chan struct{}),
		logger:            logger,
		notifHandler:      notifHandler,
		disconnectHandler: disconnectHandler,
	}
	return ns
}

// TODO: call it
func (ns *notifSubcriber) Close() {
	ns.finishedCh <- struct{}{}
}
func (ns *notifSubcriber) runSubscriber() error {
	subscriberDoneCh, err := ns.startSub()
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ns.finishedCh: // we are done
				return
			case <-subscriberDoneCh:
				// we're just disconnected from our Notif channel,
				// clear our in mem cache as we can't assume that the values
				// still updated
				ns.disconnectHandler()

				// start new subscriber
				subscriberDoneCh, err = ns.startSub()
				if err != nil {
					ns.logger.Errorf("failed to start subscriber: %v", err)
				}
			}
		}
	}()
	return nil
}

// starts subscriber to listen to all of synchronization message sent by other nodes
func (ns *notifSubcriber) startSub() (chan struct{}, error) {
	doneCh := make(chan struct{})

	// setup subscriber
	sub, err := ns.subscribe()
	if err != nil {
		close(doneCh)
		return doneCh, err
	}

	// we're just connected to our Notif channel,
	// it means we previously not connected or disconnected from the Notif channel.
	ns.disconnectHandler()

	//ns.logger.Debugf("WAITING FOR subscribed confirmation")
	switch sub.Receive().(type) {
	case redis.Subscription:
		ns.logger.Debugf("SUBSCRIBED")
	default:
		close(doneCh)
		return doneCh, fmt.Errorf("failed to subscribe")
	}
	// run subscriber loop
	go func() {
		defer func() {
			close(doneCh)
			sub.Close()
		}()

		for {
			//ns.logger.Debugf("[ns]WAITING FOR NOTIFICATION")
			vals, err := redis.Values(sub.Conn.Receive())
			if err != nil {
				ns.logger.Errorf("[ns] failed to read notification:%v", err)
				return
			}

			if len(vals) != 3 {
				ns.logger.Errorf("[ns] unexpected message array len:%v", len(vals))
				return
			}

			// first value: message type
			val1, err := redis.String(vals[0], nil)
			if val1 != "message" || err != nil {
				ns.logger.Errorf("[ns] invalid first string:%v,err:%v", val1, err)
				return
			}

			// 2nd value: channel
			val2, err := redis.String(vals[1], nil)
			if val2 != invalidationChannel || err != nil {
				ns.logger.Errorf("[ns] invalid second string:%v,err:%v", val2, err)
				return
			}

			// 3rd: keys
			val3, err := redis.Values(vals[2], nil)
			if err != nil {
				ns.logger.Errorf("[ns] unexpected third msg string:%v,err:%v", val3, err)
				return
			}
			for i, val := range val3 {
				key, err := redis.String(val, nil)
				if err != nil {
					ns.logger.Errorf("[ns] failed to read key %v: %v", i, err)
					return
				}
				//ns.logger.Debugf("----> %v adalah %v", i, key)
				ns.notifHandler(key)
			}
		}
	}()
	return doneCh, nil
}

// subscribe to the notification channel
func (ns *notifSubcriber) subscribe() (*redis.PubSubConn, error) {
	// get conn
	conn := ns.pool.Get()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	// get client ID
	id, err := redis.Int64(conn.Do("CLIENT", "ID"))
	if err != nil {
		return nil, err
	}
	ns.logger.Debugf("client ID = %v", id)

	ns.clientID = id

	sub := &redis.PubSubConn{Conn: conn}

	err = sub.Subscribe(invalidationChannel)
	if err != nil {
		sub.Close()
		return nil, err
	}

	return sub, nil
}

const (
	invalidationChannel = "__redis__:invalidate"
)
