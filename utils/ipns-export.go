package utils

import (
	"errors"
	"strings"

	"github.com/gogo/protobuf/proto"

	ipnsPb "github.com/ipfs/go-ipns/pb"
	unixfsPb "github.com/ipfs/go-unixfs/pb"
	"github.com/libp2p/go-libp2p-core/peer"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
)

type IpnsExportEntry struct {
	Name               peer.ID
	Value              string
	Signature          []byte
	ValidityType       ipnsPb.IpnsEntry_ValidityType `json:",omitempty"`
	Validity           string                        `json:",omitempty"`
	Sequence           uint64                        `json:",omitempty"`
	Ttl                uint64                        `json:",omitempty"`
	PubKey             []byte                        `json:",omitempty"`
	ReceiveTime        string                        `json:",omitempty"`
	ContentUnreachable bool                          `json:",omitempty"`
	ContentType        unixfsPb.Data_DataType        `json:",omitempty"`
	UnrecognizedType   bool                          `json:",omitempty"`
	NameResolveTime    int64                         `json:",omitempty"`
	ReceivingHead      peer.ID                       `json:",omitempty"`
}

func NewIpnsExportEntry(key string, value []byte) (IpnsExportEntry, error) {
	// Base32 Encoding of /ipns is F5UXA3TT
	if strings.HasPrefix(key, "/F5UXA3TT") || strings.HasPrefix(key, "/ipns") {
		rec := new(recpb.Record)
		err := proto.Unmarshal(value, rec)
		if err != nil {
			return IpnsExportEntry{}, err
		}

		entry := new(ipnsPb.IpnsEntry)
		err = proto.Unmarshal(rec.Value, entry)
		if err != nil {
			return IpnsExportEntry{}, err
		}

		_, pidString, err := record.SplitKey(string(rec.GetKey()))
		if err != nil {
			return IpnsExportEntry{}, err
		}

		pid, err := peer.IDFromString(pidString)
		if err != nil {
			return IpnsExportEntry{}, err
		}

		export := IpnsExportEntry{
			Name:         pid,
			Value:        string(entry.GetValue()),
			Signature:    entry.GetSignature(),
			ValidityType: entry.GetValidityType(),
			Validity:     string(entry.GetValidity()),
			Sequence:     entry.GetSequence(),
			Ttl:          entry.GetTtl(),
			PubKey:       entry.GetPubKey(),
		}
		return export, nil
	}
	return IpnsExportEntry{}, errors.New("not an IPNS record")
}
