package elastigo

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCatAliasInfo(t *testing.T) {
	Convey("catAlias Create alias line from a broken alias listing", t, func() {
		_, err := NewCatAliasInfo("production ")
		So(err, ShouldNotBeNil)
	})
	Convey("catAlias Create alias line from a complete alias listing", t, func() {
		i, err := NewCatAliasInfo("production production-2016")
		So(err, ShouldBeNil)
		So(i.Name, ShouldEqual, "production")
		So(i.Index, ShouldEqual, "production-2016")
	})
	Convey("catAlias Create alias line from an over-complete alias listing", t, func() {
		i, err := NewCatAliasInfo("production production-2016 - - -")
		So(err, ShouldBeNil)
		So(i.Name, ShouldEqual, "production")
		So(i.Index, ShouldEqual, "production-2016")
	})
}
