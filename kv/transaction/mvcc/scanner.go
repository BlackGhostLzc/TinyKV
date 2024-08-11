package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	Txn  *MvccTxn
	Iter engine_util.DBIterator

	// 可能某一个 key 有多个 write
	CurrentKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scan := &Scanner{
		Txn:        txn,
		Iter:       txn.Reader.IterCF(engine_util.CfWrite),
		CurrentKey: startKey,
	}
	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.Iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	scan.Iter.Seek(EncodeKey(scan.CurrentKey, scan.Txn.StartTS))
	if !scan.Iter.Valid() {
		return nil, nil, nil
	}
	item := scan.Iter.Item()
	gotKey := item.KeyCopy(nil)
	userKey := DecodeUserKey(gotKey)

	// 如果 userkey 和 currentkey 不同，比如说刚传进来 startkey 在数据库中不存在这个键值
	if !bytes.Equal(userKey, scan.CurrentKey) {
		scan.CurrentKey = userKey
		return scan.Next()
	}

	// 更新下一次的 CurrentKey
	for {
		scan.Iter.Next()
		if !scan.Iter.Valid() {
			break
		}
		item2 := scan.Iter.Item()
		gotKey2 := item2.KeyCopy(nil)
		userKey2 := DecodeUserKey(gotKey2)
		if !bytes.Equal(userKey2, userKey) {
			scan.CurrentKey = userKey2
			break
		}
	}

	// 获取 value
	value, err := scan.Txn.GetValue(userKey)
	if err != nil {
		return nil, nil, nil
	}

	return userKey, value, nil
}
