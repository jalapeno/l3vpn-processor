package arangodb

import (
	"context"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) iBgpL3vpnNode(ctx context.Context, key string, e *message.L3VPNPrefix) error {
	// get l3vpn node router id
	query := "for l in peer filter l.remote_ip == " + "\"" + e.Nexthop + "\""
	query += " return l"
	cursor, err := a.db.Query(ctx, query, nil)
	//glog.Infof("query: %+v", query)
	if err != nil {
		return err
	}
	var peer message.PeerStateChange
	pr, err := cursor.ReadDocument(ctx, &peer)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	obj := l3vpnNode{
		Key:         peer.RemoteBGPID + "_" + e.Nexthop,
		BGPRouterID: peer.RemoteBGPID,
		Nexthop:     e.Nexthop,
		ASN:         int32(e.PeerASN),
		//SID:         e.PrefixSID,
	}
	if _, err := a.l3vpnNode.CreateDocument(ctx, &obj); err != nil {
		glog.Infof("document: %+v, %+v", &obj, pr)
		if !driver.IsConflict(err) {
			return nil
		}
	}
	// The document already exists, updating it with the latest info
	if _, err := a.l3vpnNode.UpdateDocument(ctx, obj.Key, &obj); err != nil {
		if !driver.IsConflict(err) {
			return nil
		}
	}
	return nil
}

func (a *arangoDB) eBgpL3vpnNode(ctx context.Context, key string, e *message.L3VPNPrefix) error {
	// get internal ASN so we can determine whether this is an ebgp node or not
	getasn := "for l in ls_node_extended limit 1 return l"
	cursor, err := a.db.Query(ctx, getasn, nil)
	if err != nil {
		return err
	}

	var ln LSNodeExt
	lm, err := cursor.ReadDocument(ctx, &ln)
	glog.Infof("meta %+v", lm)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}

	if int32(e.PeerASN) == ln.ASN {
		glog.Infof("internal l3vpn learned from ebgp BMP node, do not process: %+v, peer_as: %+v, ln.ASN: %+v", e.Prefix, e.PeerASN, ln.ASN)
		//return a.processUnicastPrefixRemoval(ctx, key)
		return nil

	} else {
		query := "for l in peer filter l.remote_ip == " + "\"" + e.PeerIP + "\""
		query += " return l"
		cursor, err := a.db.Query(ctx, query, nil)
		//glog.Infof("query: %+v", query)
		if err != nil {
			return err
		}
		var peer message.PeerStateChange
		pr, err := cursor.ReadDocument(ctx, &peer)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
		}
		obj := l3vpnNode{
			Key:         peer.RemoteBGPID + "_" + e.Nexthop,
			BGPRouterID: peer.RemoteBGPID,
			Nexthop:     e.Nexthop,
			ASN:         int32(e.PeerASN),
			//SID:         e.PrefixSID,
		}
		if _, err := a.l3vpnNode.CreateDocument(ctx, &obj); err != nil {
			glog.Infof("document: %+v, %+v", &obj, pr)
			if !driver.IsConflict(err) {
				return nil
			}
		}
		// The document already exists, updating it with the latest info
		if _, err := a.l3vpnNode.UpdateDocument(ctx, obj.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return nil
			}
		}
	}
	return nil
}
