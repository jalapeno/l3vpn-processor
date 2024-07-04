package arangodb

import (
	"context"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) iBgpL3vpnV4PrefixEdge(ctx context.Context, key string, e *message.L3VPNPrefix) error {
	// get internal ASN so we can determine whether this is an external prefix or not
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

	if int32(e.PeerASN) != ln.ASN {
		glog.Infof("iBGP l3vpn prefix learned from eBGP BMP node, do not process: %+v, peer_as: %+v, ln.ASN: %+v", e.Prefix, e.PeerASN, ln.ASN)
		//return a.processUnicastPrefixRemoval(ctx, key)
		return nil

	} else {
		query := "for l in l3vpn_node filter l.nexthop == " + e.Nexthop
		query += " return l	"
		pcursor, err := a.db.Query(ctx, query, nil)
		if err != nil {
			return err
		}
		defer pcursor.Close()
		for {
			var node l3vpnNode
			mp, err := pcursor.ReadDocument(ctx, &node)
			if err != nil {
				if driver.IsNoMoreDocuments(err) {
					return err
				}
				if !driver.IsNoMoreDocuments(err) {
					return err
				}
				break
			}

			glog.Infof("l3vpn node %s + iBGP l3vpn prefix %s, meta %+v", e.Key, node.Key, mp)
			from := l3vpnPrefixEdgeObject{
				Key:       node.Key + "_" + e.Key,
				From:      node.ID,
				To:        e.ID,
				Prefix:    e.Prefix,
				PrefixLen: e.PrefixLen,
				Nexthop:   e.Nexthop,
				PeerASN:   e.PeerASN,
				Labels:    e.Labels,
				SID:       e.PrefixSID,
			}

			if _, err := a.graph.CreateDocument(ctx, &from); err != nil {

				if !driver.IsConflict(err) {
					return err
				}
				// The document already exists, updating it with the latest info
				if _, err := a.graph.UpdateDocument(ctx, from.Key, &from); err != nil {
					return err
				}
			}
			to := l3vpnPrefixEdgeObject{
				Key:       e.Key + "_" + node.Key,
				From:      e.ID,
				To:        node.ID,
				Prefix:    e.Prefix,
				PrefixLen: e.PrefixLen,
				PeerASN:   e.PeerASN,
			}

			if _, err := a.graph.CreateDocument(ctx, &to); err != nil {
				if !driver.IsConflict(err) {
					return err
				}
				// The document already exists, updating it with the latest info
				if _, err := a.graph.UpdateDocument(ctx, to.Key, &to); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// func (a *arangoDB) iBgpL3vpnV4PrefixEdge(ctx context.Context, key string, e *message.L3VPNPrefix) error {
// 	// get internal ASN so we can determine whether this is an external prefix or not
// 	getasn := "for l in ls_node_extended limit 1 return l"
// 	cursor, err := a.db.Query(ctx, getasn, nil)
// 	if err != nil {
// 		return err
// 	}
// 	var ln LSNodeExt
// 	lm, err := cursor.ReadDocument(ctx, &ln)
// 	glog.Infof("meta %+v", lm)
// 	if err != nil {
// 		if !driver.IsNoMoreDocuments(err) {
// 			return err
// 		}
// 	}

// 	if e.OriginAS == ln.ASN {
// 		glog.V(5).Infof("internal prefix, do not process: %+v, origin_as: %+v, ln.ASN: %+v", e.Prefix, e.OriginAS, ln.ASN)
// 		return a.processInet4Removal(ctx, key, e)

// 	} else {
// 		obj := inetPrefix{
// 			//Key: inetKey,
// 			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
// 			Prefix:    e.Prefix,
// 			PrefixLen: e.PrefixLen,
// 			OriginAS:  e.OriginAS,
// 			NextHop:   e.Nexthop,
// 		}
// 		if _, err := a.inetprefixV4.CreateDocument(ctx, &obj); err != nil {
// 			//glog.Infof("adding prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
// 			if !driver.IsConflict(err) {
// 				return nil
// 			}
// 		}
// 		// The document already exists, updating it with the latest info
// 		if _, err := a.inetprefixV4.UpdateDocument(ctx, ln.Key, &obj); err != nil {
// 			if !driver.IsConflict(err) {
// 				return nil
// 			}
// 		}
// 	}
// 	return nil
// }

func (a *arangoDB) eBgpL3vpnV4PrefixEdge(ctx context.Context, key string, e *message.L3VPNPrefix) error {
	// get internal ASN so we can determine whether this is an iBGP sourced prefix or not
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
		glog.V(5).Infof("iBGP l3vpn prefix, do not process: %+v, origin_as: %+v, ln.ASN: %+v", e.Prefix, e.OriginAS, ln.ASN)
		// return a.processInet4Removal(ctx, key, e)
		return nil

	} else {
		obj := inetPrefix{
			//Key: inetKey,
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
			NextHop:   e.Nexthop,
		}
		if _, err := a.inetprefixV4.CreateDocument(ctx, &obj); err != nil {
			//glog.Infof("adding prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return nil
			}
		}
		// The document already exists, updating it with the latest info
		if _, err := a.inetprefixV4.UpdateDocument(ctx, ln.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return nil
			}
		}
	}
	return nil
}

// process Removal removes records from the inetprefixV4 collection
func (a *arangoDB) processInet4Removal(ctx context.Context, key string, e *message.L3VPNPrefix) error {
	query := "for d in " + a.inetprefixV4.Name() +
		" filter d.prefix == " + "\"" + e.Prefix + "\""
	query += " return d"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm inetPrefix
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.inetprefixV4.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (a *arangoDB) processInet6(ctx context.Context, key, id string, e message.L3VPNPrefix) error {
	// get internal ASN so we can determine whether this is an external prefix or not
	getasn := "for l in ls_node_extended limit 1 return l"
	cursor, err := a.db.Query(ctx, getasn, nil)
	if err != nil {
		return err
	}

	var ln LSNodeExt
	lm, err := cursor.ReadDocument(ctx, &ln)
	glog.V(5).Infof("meta %+v", lm)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}

	var result bool = false
	for _, x := range e.BaseAttributes.ASPath {
		if x == uint32(ln.ASN) {
			result = true
			break
		}
	}
	if result {
		glog.V(5).Infof("internal ASN %+v found in unicast prefix, do not process", e.Prefix)
	}

	//glog.Infof("got message %+v", &e)
	if e.OriginAS == ln.ASN {
		//glog.Infof("internal prefix, do not process: %+v, origin_as: %+v, ln.ASN: %+v", e.Prefix, e.OriginAS, ln.ASN)
		return a.processInet6Removal(ctx, key, &e)
	}
	if e.OriginAS == 0 {
		//glog.Infof("internal prefix, do not process: %+v, origin_as: %+v, ln.ASN: %+v", e.Prefix, e.OriginAS, ln.ASN)
		return a.processInet4Removal(ctx, key, &e)

	} else {
		obj := inetPrefix{
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
			NextHop:   e.Nexthop,
		}
		if _, err := a.inetprefixV6.CreateDocument(ctx, &obj); err != nil {
			//glog.Infof("adding prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return nil
			}
		}
		// The document already exists, updating it with the latest info
		if _, err := a.inetprefixV6.UpdateDocument(ctx, ln.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return nil
			}
		}
	}
	return nil
}

// process Removal removes records from the inetprefixV4 collection
func (a *arangoDB) processInet6Removal(ctx context.Context, key string, e *message.L3VPNPrefix) error {
	query := "for d in " + a.inetprefixV6.Name() +
		" filter d._key == " + "\"" + key + "\""
	query += " return d"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm inetPrefix
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.inetprefixV6.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}
