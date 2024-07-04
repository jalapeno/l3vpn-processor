package arangodb

import (
	"context"
	"encoding/json"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/jalapeno/topology/pkg/dbclient"
	notifier "github.com/jalapeno/topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/tools"
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop chan struct{}

	inetprefixV4      driver.Collection
	inetprefixV6      driver.Collection
	graph             driver.Collection
	l3vpnNode         driver.Collection
	l3vpnPrefixV4     driver.Collection
	l3vpnPrefixV6     driver.Collection
	l3vpnPrefixV4Edge driver.Graph
	l3vpnPrefixV6Edge driver.Graph
	vrf               driver.Collection
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, peer, bgpNode string, unicastprefixV4, unicastprefixV6,
	ebgpprefixV4, ebgpprefixV6, inetprefixV4 string, inetprefixV6 string,
	l3vpnNode string, l3vpnPrefixV4 string, l3vpnPrefixV6 string, l3vpnPrefixV4Edge string, l3vpnPrefixV6Edge string, vrf string) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop: make(chan struct{}),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn

	// Check if l3vpn_v4_prefix collection exists, if not fail as Jalapeno topology is not running
	arango.l3vpnPrefixV4, err = arango.db.Collection(context.TODO(), l3vpnPrefixV4)
	if err != nil {
		return nil, err
	}
	// Check if l3vpn_v6_prefix collection exists, if not fail as Jalapeno topology is not running
	arango.l3vpnPrefixV6, err = arango.db.Collection(context.TODO(), l3vpnPrefixV6)
	if err != nil {
		return nil, err
	}

	// Check if l3vpn_v4_prefix_edge collection exists, if not fail as Jalapeno topology is not running
	arango.l3vpnPrefixV4Edge, err = arango.db.Graph(context.TODO(), l3vpnPrefixV4Edge)
	if err != nil {
		return nil, err
	}
	// Check if l3vpn_v6_prefix_edge collection exists, if not fail as Jalapeno topology is not running
	arango.l3vpnPrefixV6Edge, err = arango.db.Graph(context.TODO(), l3vpnPrefixV6Edge)
	if err != nil {
		return nil, err
	}

	// check for l3vpn_node collection
	found, err := arango.db.CollectionExists(context.TODO(), l3vpnNode)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), l3vpnNode)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// create l3vpn_node collection
	var l3vpnNode_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.l3vpnNode, err = arango.db.CreateCollection(context.TODO(), "l3vpn_node", l3vpnNode_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.l3vpnNode, err = arango.db.Collection(context.TODO(), l3vpnNode)
	if err != nil {
		return nil, err
	}
	return arango, nil
}

func (a *arangoDB) Start() error {
	if err := a.loadCollection(); err != nil {
		return err
	}
	glog.Infof("Connected to arango database, starting monitor")
	go a.monitor()

	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType dbclient.CollectionType, msg []byte) error {
	event := &notifier.EventMessage{}
	if err := json.Unmarshal(msg, event); err != nil {
		return err
	}
	event.TopicType = msgType
	switch msgType {
	// case bmp.PeerStateChangeMsg:
	// 	return a.peerHandler(event)
	case bmp.L3VPNV4Msg:
		return a.l3vpnV4Handler(event)
	case bmp.L3VPNV6Msg:
		return a.l3vpnV6Handler(event)
	}
	return nil
}

func (a *arangoDB) monitor() {
	for {
		select {
		case <-a.stop:
			return
		}
	}
}

func (a *arangoDB) loadCollection() error {
	ctx := context.TODO()

	l3vpn_ibgp_node_query := "for l in l3vpn_v4_prefix filter l.base_attrs.as_path_count == 1 && l.base_attrs.local_pref != null return l"
	cursor, err := a.db.Query(ctx, l3vpn_ibgp_node_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.L3VPNPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.Infof("processing iBGP l3vpn node: %+v", p.Nexthop)
		if err := a.iBgpL3vpnNode(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
		glog.Infof("processing iBGP l3vpn prefix: %+v", p.Prefix)
		if err := a.iBgpL3vpnV4PrefixEdge(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}

	}

	l3vpn_ebgp_node_query := "for l in l3vpn_v4_prefix filter l.base_attrs.as_path_count == 1 && l.base_attrs.local_pref == null return l"
	cursor, err = a.db.Query(ctx, l3vpn_ebgp_node_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.L3VPNPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.Infof("processing eBGP l3vpn node: %+v", p.Nexthop)
		if err := a.eBgpL3vpnNode(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
		glog.Infof("processing eBGP l3vpn prefix: %+v", p.Prefix)
		if err := a.eBgpL3vpnV4PrefixEdge(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	return nil
}
