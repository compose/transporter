package etcd

import "testing"

func TestClose(t *testing.T) {
	// this should do absolutely nothing but if other tests break, it's possibly doing something
	// it shouldn't
	defaultSession.Close()
}
