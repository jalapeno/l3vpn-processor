package arangodb

import (
	"context"
	"fmt"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	notifier "github.com/jalapeno/topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) l3vpnV4Handler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	//glog.Infof("handler obj: %+v", obj)
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}

	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.l3vpnPrefixV4.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.l3vpnPrefixV4.Name(), c)
	}
	//glog.Infof("Processing unicast prefix action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.L3VPNPrefix
	_, err := a.l3vpnPrefixV4.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a prefix removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processInet4Removal(ctx, obj.Key, &o)
	}
	switch obj.Action {
	case "add":
		fallthrough
	case "update":
		//glog.Infof("passing to processInet: %+v", o)
		//if err := a.processL3vpnNode(ctx, obj.Key, obj.ID, o); err != nil {
		if err := a.iBgpL3vpnV4PrefixEdge(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for vertex %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
	}
	return nil
}

func (a *arangoDB) l3vpnV6Handler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.l3vpnPrefixV6.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.l3vpnPrefixV6.Name(), c)
	}
	glog.V(6).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.L3VPNPrefix
	_, err := a.l3vpnPrefixV6.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a prefix removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processInet6Removal(ctx, obj.Key, &o)
	}
	switch obj.Action {
	case "add":
		fallthrough
	case "update":
		if err := a.processInet6(ctx, obj.Key, obj.ID, o); err != nil {
			return fmt.Errorf("failed to process action %s for vertex %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
	}
	return nil
}
