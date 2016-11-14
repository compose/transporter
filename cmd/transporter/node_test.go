package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/compose/transporter/pkg/adaptor"
)

func TestFind(t *testing.T) {
	// build a tree to run tests on
	//   root
	//   /  \
	// c1    c2
	//  |
	// c1c1
	root, _ := NewNode("root", "test", adaptor.Config{})
	c1, _ := NewNode("c1", "test", adaptor.Config{})
	c2, _ := NewNode("c2", "test", adaptor.Config{})
	c1c1, _ := NewNode("c1c1", "test", adaptor.Config{})

	root.Add(&c1)
	root.Add(&c2)
	c1.Add(&c1c1)

	data := []struct {
		description string
		in          Node
		searchUUID  string
		out         Node
		pass        bool
		err         error
	}{
		{
			"find root from root node",
			root,
			root.UUID,
			root,
			true,
			errors.New(""),
		},
		{
			"find c1 from root node",
			root,
			c1.UUID,
			c1,
			true,
			errors.New(""),
		},
		{
			"find c2 from root node",
			root,
			c2.UUID,
			c2,
			true,
			errors.New(""),
		},
		{
			"find c2 from c1",
			c1,
			c2.UUID,
			c2,
			false,
			fmt.Errorf("child %s not found under %s", c2.UUID, c1.UUID),
		},
		{
			"find c1c1 from root",
			root,
			c1c1.UUID,
			c1c1,
			true,
			errors.New(""),
		},
		{
			"find c1c1 from c1",
			c1,
			c1c1.UUID,
			c1c1,
			true,
			errors.New(""),
		},
		{
			"find c1c1 from c2",
			c2,
			c1c1.UUID,
			c1c1,
			false,
			fmt.Errorf("child %s not found under %s", c1c1.UUID, c2.UUID),
		},
	}

	for _, v := range data {
		res, err := v.in.Find(v.searchUUID)
		if v.pass && (err == nil) {
			if res.Name != v.out.Name {
				t.Errorf("%s: found %s expected %s", v.description, res.Name, v.in.Name)
			}
		} else {
			if err != nil && err.Error() != v.err.Error() {
				t.Errorf("%s: %s expected %s", v.description, err, v.err)
			}
		}
	}
}
