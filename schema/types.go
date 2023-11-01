package schema

import (
	"errors"
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/bindnode"
)

type (
	DBMetaInfo struct {
		Version int64
		Format  string
	}

	IndexMetaInfo struct {
		Version int64
	}
)

func BuildDBMetaInfoNode(version int64, format string) (ipld.Node, error) {
	dbMetaInfo := &DBMetaInfo{
		Version: version,
		Format:  format,
	}
	return dbMetaInfo.ToNode()
}

func (dmi DBMetaInfo) ToNode() (n ipld.Node, err error) {
	// TODO: remove the panic recovery once IPLD bindnode is stabilized.
	defer func() {
		if r := recover(); r != nil {
			err = toError(r)
		}
	}()
	n = bindnode.Wrap(&dmi, DBMetaInfoPrototype.Type()).Representation()
	return
}

func UnwrapDBMetaInfo(node ipld.Node) (*DBMetaInfo, error) {
	if node.Prototype() != DBMetaInfoPrototype {
		dmiBuilder := DBMetaInfoPrototype.NewBuilder()
		err := dmiBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = dmiBuilder.Build()
	}

	dmi, ok := bindnode.Unwrap(node).(*DBMetaInfo)
	if !ok || dmi == nil {
		return nil, fmt.Errorf("unwrapped node does not match schema.DBMetaInfo")
	}
	return dmi, nil
}

func BuildIndexMetaInfoNode(version int64) (ipld.Node, error) {
	indexMetaInfo := &IndexMetaInfo{Version: version}
	return indexMetaInfo.ToNode()
}

func (imi IndexMetaInfo) ToNode() (n ipld.Node, err error) {
	// TODO: remove the panic recovery once IPLD bindnode is stabilized.
	defer func() {
		if r := recover(); r != nil {
			err = toError(r)
		}
	}()
	n = bindnode.Wrap(&imi, IndexMetaInfoPrototype.Type()).Representation()
	return
}

func UnwrapIndexMetaInfo(node ipld.Node) (*IndexMetaInfo, error) {
	if node.Prototype() != IndexMetaInfoPrototype {
		imiBuilder := IndexMetaInfoPrototype.NewBuilder()
		err := imiBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = imiBuilder.Build()
	}

	imi, ok := bindnode.Unwrap(node).(*IndexMetaInfo)
	if !ok || imi == nil {
		return nil, fmt.Errorf("unwrapped node does not match schema.IndexMetaInfo")
	}
	return imi, nil
}

func toError(r interface{}) error {
	switch x := r.(type) {
	case string:
		return errors.New(x)
	case error:
		return x
	default:
		return fmt.Errorf("unknown panic: %v", r)
	}
}
