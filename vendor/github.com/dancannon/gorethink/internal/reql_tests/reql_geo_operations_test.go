// Code generated by gen_tests.py and process_polyglot.py.
// Do not edit this file directly.
// The template for this file is located at:
// ../template.go.tpl
package reql_tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	r "gopkg.in/gorethink/gorethink.v2"
	"gopkg.in/gorethink/gorethink.v2/internal/compare"
)

// Test basic geometry operators
func TestGeoOperationsSuite(t *testing.T) {
	suite.Run(t, new(GeoOperationsSuite))
}

type GeoOperationsSuite struct {
	suite.Suite

	session *r.Session
}

func (suite *GeoOperationsSuite) SetupTest() {
	suite.T().Log("Setting up GeoOperationsSuite")
	// Use imports to prevent errors
	_ = time.Time{}
	_ = compare.AnythingIsFine

	session, err := r.Connect(r.ConnectOpts{
		Address: url,
	})
	suite.Require().NoError(err, "Error returned when connecting to server")
	suite.session = session

	r.DBDrop("test").Exec(suite.session)
	err = r.DBCreate("test").Exec(suite.session)
	suite.Require().NoError(err)
	err = r.DB("test").Wait().Exec(suite.session)
	suite.Require().NoError(err)

}

func (suite *GeoOperationsSuite) TearDownSuite() {
	suite.T().Log("Tearing down GeoOperationsSuite")

	if suite.session != nil {
		r.DB("rethinkdb").Table("_debug_scratch").Delete().Exec(suite.session)
		r.DBDrop("test").Exec(suite.session)

		suite.session.Close()
	}
}

func (suite *GeoOperationsSuite) TestCases() {
	suite.T().Log("Running GeoOperationsSuite: Test basic geometry operators")

	{
		// geo/operations.yaml line #5
		/* ("89011.26253835332") */
		var expected_ string = "89011.26253835332"
		/* r.distance(r.point(-122, 37), r.point(-123, 37)).coerce_to('STRING') */

		suite.T().Log("About to run line #5: r.Distance(r.Point(-122, 37), r.Point(-123, 37)).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(-122, 37), r.Point(-123, 37)).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #5")
	}

	{
		// geo/operations.yaml line #7
		/* ("110968.30443995494") */
		var expected_ string = "110968.30443995494"
		/* r.distance(r.point(-122, 37), r.point(-122, 36)).coerce_to('STRING') */

		suite.T().Log("About to run line #7: r.Distance(r.Point(-122, 37), r.Point(-122, 36)).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(-122, 37), r.Point(-122, 36)).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #7")
	}

	{
		// geo/operations.yaml line #9
		/* true */
		var expected_ bool = true
		/* r.distance(r.point(-122, 37), r.point(-122, 36)).eq(r.distance(r.point(-122, 36), r.point(-122, 37))) */

		suite.T().Log("About to run line #9: r.Distance(r.Point(-122, 37), r.Point(-122, 36)).Eq(r.Distance(r.Point(-122, 36), r.Point(-122, 37)))")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(-122, 37), r.Point(-122, 36)).Eq(r.Distance(r.Point(-122, 36), r.Point(-122, 37))), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #9")
	}

	{
		// geo/operations.yaml line #11
		/* ("89011.26253835332") */
		var expected_ string = "89011.26253835332"
		/* r.point(-122, 37).distance(r.point(-123, 37)).coerce_to('STRING') */

		suite.T().Log("About to run line #11: r.Point(-122, 37).Distance(r.Point(-123, 37)).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Point(-122, 37).Distance(r.Point(-123, 37)).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #11")
	}

	// geo/operations.yaml line #13
	// someDist = r.distance(r.point(-122, 37), r.point(-123, 37))
	suite.T().Log("Possibly executing: var someDist r.Term = r.Distance(r.Point(-122, 37), r.Point(-123, 37))")

	someDist := r.Distance(r.Point(-122, 37), r.Point(-123, 37))
	_ = someDist // Prevent any noused variable errors

	{
		// geo/operations.yaml line #15
		/* true */
		var expected_ bool = true
		/* someDist.eq(r.distance(r.point(-122, 37), r.point(-123, 37), unit='m')) */

		suite.T().Log("About to run line #15: someDist.Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{Unit: 'm', }))")

		runAndAssert(suite.Suite, expected_, someDist.Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{Unit: "m"})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #15")
	}

	{
		// geo/operations.yaml line #19
		/* true */
		var expected_ bool = true
		/* someDist.mul(1.0/1000.0).eq(r.distance(r.point(-122, 37), r.point(-123, 37), unit='km')) */

		suite.T().Log("About to run line #19: someDist.Mul(r.Div(1.0, 1000.0)).Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{Unit: 'km', }))")

		runAndAssert(suite.Suite, expected_, someDist.Mul(r.Div(1.0, 1000.0)).Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{Unit: "km"})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #19")
	}

	{
		// geo/operations.yaml line #23
		/* true */
		var expected_ bool = true
		/* someDist.mul(1.0/1609.344).eq(r.distance(r.point(-122, 37), r.point(-123, 37), unit='mi')) */

		suite.T().Log("About to run line #23: someDist.Mul(r.Div(1.0, 1609.344)).Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{Unit: 'mi', }))")

		runAndAssert(suite.Suite, expected_, someDist.Mul(r.Div(1.0, 1609.344)).Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{Unit: "mi"})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #23")
	}

	{
		// geo/operations.yaml line #27
		/* true */
		var expected_ bool = true
		/* someDist.mul(1.0/0.3048).eq(r.distance(r.point(-122, 37), r.point(-123, 37), unit='ft')) */

		suite.T().Log("About to run line #27: someDist.Mul(r.Div(1.0, 0.3048)).Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{Unit: 'ft', }))")

		runAndAssert(suite.Suite, expected_, someDist.Mul(r.Div(1.0, 0.3048)).Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{Unit: "ft"})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #27")
	}

	{
		// geo/operations.yaml line #31
		/* true */
		var expected_ bool = true
		/* someDist.mul(1.0/1852.0).eq(r.distance(r.point(-122, 37), r.point(-123, 37), unit='nm')) */

		suite.T().Log("About to run line #31: someDist.Mul(r.Div(1.0, 1852.0)).Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{Unit: 'nm', }))")

		runAndAssert(suite.Suite, expected_, someDist.Mul(r.Div(1.0, 1852.0)).Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{Unit: "nm"})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #31")
	}

	{
		// geo/operations.yaml line #35
		/* true */
		var expected_ bool = true
		/* someDist.eq(r.distance(r.point(-122, 37), r.point(-123, 37), geo_system='WGS84')) */

		suite.T().Log("About to run line #35: someDist.Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{GeoSystem: 'WGS84', }))")

		runAndAssert(suite.Suite, expected_, someDist.Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{GeoSystem: "WGS84"})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #35")
	}

	{
		// geo/operations.yaml line #40
		/* true */
		var expected_ bool = true
		/* someDist.div(10).eq(r.distance(r.point(-122, 37), r.point(-123, 37), geo_system={'a':637813.7, 'f':(1.0/298.257223563)})) */

		suite.T().Log("About to run line #40: someDist.Div(10).Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{GeoSystem: map[interface{}]interface{}{'a': 637813.7, 'f': r.Div(1.0, 298.257223563), }, }))")

		runAndAssert(suite.Suite, expected_, someDist.Div(10).Eq(r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{GeoSystem: map[interface{}]interface{}{"a": 637813.7, "f": r.Div(1.0, 298.257223563)}})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #40")
	}

	{
		// geo/operations.yaml line #43
		/* ("0.01393875509649327") */
		var expected_ string = "0.01393875509649327"
		/* r.distance(r.point(-122, 37), r.point(-123, 37), geo_system='unit_sphere').coerce_to('STRING') */

		suite.T().Log("About to run line #43: r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{GeoSystem: 'unit_sphere', }).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(-122, 37), r.Point(-123, 37)).OptArgs(r.DistanceOpts{GeoSystem: "unit_sphere"}).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #43")
	}

	{
		// geo/operations.yaml line #47
		/* ("0") */
		var expected_ string = "0"
		/* r.distance(r.point(0, 0), r.point(0, 0)).coerce_to('STRING') */

		suite.T().Log("About to run line #47: r.Distance(r.Point(0, 0), r.Point(0, 0)).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(0, 0), r.Point(0, 0)).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #47")
	}

	{
		// geo/operations.yaml line #50
		/* ("40007862.917250897") */
		var expected_ string = "40007862.917250897"
		/* r.distance(r.point(0, 0), r.point(180, 0)).mul(2).coerce_to('STRING') */

		suite.T().Log("About to run line #50: r.Distance(r.Point(0, 0), r.Point(180, 0)).Mul(2).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(0, 0), r.Point(180, 0)).Mul(2).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #50")
	}

	{
		// geo/operations.yaml line #52
		/* ("40007862.917250897") */
		var expected_ string = "40007862.917250897"
		/* r.distance(r.point(0, -90), r.point(0, 90)).mul(2).coerce_to('STRING') */

		suite.T().Log("About to run line #52: r.Distance(r.Point(0, -90), r.Point(0, 90)).Mul(2).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(0, -90), r.Point(0, 90)).Mul(2).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #52")
	}

	{
		// geo/operations.yaml line #54
		/* ("0") */
		var expected_ string = "0"
		/* r.distance(r.point(0, 0), r.line([0,0], [0,1])).coerce_to('STRING') */

		suite.T().Log("About to run line #54: r.Distance(r.Point(0, 0), r.Line([]interface{}{0, 0}, []interface{}{0, 1})).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(0, 0), r.Line([]interface{}{0, 0}, []interface{}{0, 1})).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #54")
	}

	{
		// geo/operations.yaml line #56
		/* ("0") */
		var expected_ string = "0"
		/* r.distance(r.line([0,0], [0,1]), r.point(0, 0)).coerce_to('STRING') */

		suite.T().Log("About to run line #56: r.Distance(r.Line([]interface{}{0, 0}, []interface{}{0, 1}), r.Point(0, 0)).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Line([]interface{}{0, 0}, []interface{}{0, 1}), r.Point(0, 0)).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #56")
	}

	{
		// geo/operations.yaml line #58
		/* true */
		var expected_ bool = true
		/* r.distance(r.point(0, 0), r.line([0.1,0], [1,0])).eq(r.distance(r.point(0, 0), r.point(0.1, 0))) */

		suite.T().Log("About to run line #58: r.Distance(r.Point(0, 0), r.Line([]interface{}{0.1, 0}, []interface{}{1, 0})).Eq(r.Distance(r.Point(0, 0), r.Point(0.1, 0)))")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(0, 0), r.Line([]interface{}{0.1, 0}, []interface{}{1, 0})).Eq(r.Distance(r.Point(0, 0), r.Point(0.1, 0))), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #58")
	}

	{
		// geo/operations.yaml line #60
		/* ("492471.4990055255") */
		var expected_ string = "492471.4990055255"
		/* r.distance(r.point(0, 0), r.line([5,-1], [4,2])).coerce_to('STRING') */

		suite.T().Log("About to run line #60: r.Distance(r.Point(0, 0), r.Line([]interface{}{5, -1}, []interface{}{4, 2})).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(0, 0), r.Line([]interface{}{5, -1}, []interface{}{4, 2})).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #60")
	}

	{
		// geo/operations.yaml line #62
		/* ("492471.4990055255") */
		var expected_ string = "492471.4990055255"
		/* r.distance(r.point(0, 0), r.polygon([5,-1], [4,2], [10,10])).coerce_to('STRING') */

		suite.T().Log("About to run line #62: r.Distance(r.Point(0, 0), r.Polygon([]interface{}{5, -1}, []interface{}{4, 2}, []interface{}{10, 10})).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(0, 0), r.Polygon([]interface{}{5, -1}, []interface{}{4, 2}, []interface{}{10, 10})).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #62")
	}

	{
		// geo/operations.yaml line #64
		/* ("0") */
		var expected_ string = "0"
		/* r.distance(r.point(0, 0), r.polygon([0,-1], [0,1], [10,10])).coerce_to('STRING') */

		suite.T().Log("About to run line #64: r.Distance(r.Point(0, 0), r.Polygon([]interface{}{0, -1}, []interface{}{0, 1}, []interface{}{10, 10})).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(0, 0), r.Polygon([]interface{}{0, -1}, []interface{}{0, 1}, []interface{}{10, 10})).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #64")
	}

	{
		// geo/operations.yaml line #66
		/* ("0") */
		var expected_ string = "0"
		/* r.distance(r.point(0.5, 0.5), r.polygon([0,-1], [0,1], [10,10])).coerce_to('STRING') */

		suite.T().Log("About to run line #66: r.Distance(r.Point(0.5, 0.5), r.Polygon([]interface{}{0, -1}, []interface{}{0, 1}, []interface{}{10, 10})).CoerceTo('STRING')")

		runAndAssert(suite.Suite, expected_, r.Distance(r.Point(0.5, 0.5), r.Polygon([]interface{}{0, -1}, []interface{}{0, 1}, []interface{}{10, 10})).CoerceTo("STRING"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #66")
	}

	{
		// geo/operations.yaml line #71
		/* false */
		var expected_ bool = false
		/* r.circle([0,0], 1, fill=false).eq(r.circle([0,0], 1, fill=true)) */

		suite.T().Log("About to run line #71: r.Circle([]interface{}{0, 0}, 1).OptArgs(r.CircleOpts{Fill: false, }).Eq(r.Circle([]interface{}{0, 0}, 1).OptArgs(r.CircleOpts{Fill: true, }))")

		runAndAssert(suite.Suite, expected_, r.Circle([]interface{}{0, 0}, 1).OptArgs(r.CircleOpts{Fill: false}).Eq(r.Circle([]interface{}{0, 0}, 1).OptArgs(r.CircleOpts{Fill: true})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #71")
	}

	{
		// geo/operations.yaml line #75
		/* true */
		var expected_ bool = true
		/* r.circle([0,0], 1, fill=false).fill().eq(r.circle([0,0], 1, fill=true)) */

		suite.T().Log("About to run line #75: r.Circle([]interface{}{0, 0}, 1).OptArgs(r.CircleOpts{Fill: false, }).Fill().Eq(r.Circle([]interface{}{0, 0}, 1).OptArgs(r.CircleOpts{Fill: true, }))")

		runAndAssert(suite.Suite, expected_, r.Circle([]interface{}{0, 0}, 1).OptArgs(r.CircleOpts{Fill: false}).Fill().Eq(r.Circle([]interface{}{0, 0}, 1).OptArgs(r.CircleOpts{Fill: true})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #75")
	}

	{
		// geo/operations.yaml line #80
		/* ({'$reql_type$':'GEOMETRY', 'coordinates':[[[0,0],[1,0],[1,1],[0,1],[0,0]],[[0.1,0.1],[0.9,0.1],[0.9,0.9],[0.1,0.9],[0.1,0.1]]], 'type':'Polygon'}) */
		var expected_ map[interface{}]interface{} = map[interface{}]interface{}{"$reql_type$": "GEOMETRY", "coordinates": []interface{}{[]interface{}{[]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}, []interface{}{0, 0}}, []interface{}{[]interface{}{0.1, 0.1}, []interface{}{0.9, 0.1}, []interface{}{0.9, 0.9}, []interface{}{0.1, 0.9}, []interface{}{0.1, 0.1}}}, "type": "Polygon"}
		/* r.polygon([0,0], [1,0], [1,1], [0,1]).polygon_sub(r.polygon([0.1,0.1], [0.9,0.1], [0.9,0.9], [0.1,0.9])) */

		suite.T().Log("About to run line #80: r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0.1, 0.1}, []interface{}{0.9, 0.1}, []interface{}{0.9, 0.9}, []interface{}{0.1, 0.9}))")

		runAndAssert(suite.Suite, expected_, r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0.1, 0.1}, []interface{}{0.9, 0.1}, []interface{}{0.9, 0.9}, []interface{}{0.1, 0.9})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #80")
	}

	{
		// geo/operations.yaml line #82
		/* err('ReqlQueryLogicError', 'The second argument to `polygon_sub` is not contained in the first one.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "The second argument to `polygon_sub` is not contained in the first one.")
		/* r.polygon([0,0], [1,0], [1,1], [0,1]).polygon_sub(r.polygon([0.1,0.9], [0.9,0.0], [0.9,0.9], [0.1,0.9])) */

		suite.T().Log("About to run line #82: r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0.1, 0.9}, []interface{}{0.9, 0.0}, []interface{}{0.9, 0.9}, []interface{}{0.1, 0.9}))")

		runAndAssert(suite.Suite, expected_, r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0.1, 0.9}, []interface{}{0.9, 0.0}, []interface{}{0.9, 0.9}, []interface{}{0.1, 0.9})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #82")
	}

	{
		// geo/operations.yaml line #84
		/* err('ReqlQueryLogicError', 'The second argument to `polygon_sub` is not contained in the first one.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "The second argument to `polygon_sub` is not contained in the first one.")
		/* r.polygon([0,0], [1,0], [1,1], [0,1]).polygon_sub(r.polygon([0,0], [2,0], [2,2], [0,2])) */

		suite.T().Log("About to run line #84: r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0, 0}, []interface{}{2, 0}, []interface{}{2, 2}, []interface{}{0, 2}))")

		runAndAssert(suite.Suite, expected_, r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0, 0}, []interface{}{2, 0}, []interface{}{2, 2}, []interface{}{0, 2})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #84")
	}

	{
		// geo/operations.yaml line #86
		/* err('ReqlQueryLogicError', 'The second argument to `polygon_sub` is not contained in the first one.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "The second argument to `polygon_sub` is not contained in the first one.")
		/* r.polygon([0,0], [1,0], [1,1], [0,1]).polygon_sub(r.polygon([0,-2], [1,-2], [-1,1], [0,-1])) */

		suite.T().Log("About to run line #86: r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0, -2}, []interface{}{1, -2}, []interface{}{-1, 1}, []interface{}{0, -1}))")

		runAndAssert(suite.Suite, expected_, r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0, -2}, []interface{}{1, -2}, []interface{}{-1, 1}, []interface{}{0, -1})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #86")
	}

	{
		// geo/operations.yaml line #88
		/* err('ReqlQueryLogicError', 'The second argument to `polygon_sub` is not contained in the first one.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "The second argument to `polygon_sub` is not contained in the first one.")
		/* r.polygon([0,0], [1,0], [1,1], [0,1]).polygon_sub(r.polygon([0,-1], [1,-1], [1,0], [0,0])) */

		suite.T().Log("About to run line #88: r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0, -1}, []interface{}{1, -1}, []interface{}{1, 0}, []interface{}{0, 0}))")

		runAndAssert(suite.Suite, expected_, r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0, -1}, []interface{}{1, -1}, []interface{}{1, 0}, []interface{}{0, 0})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #88")
	}

	{
		// geo/operations.yaml line #90
		/* err('ReqlQueryLogicError', 'The second argument to `polygon_sub` is not contained in the first one.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "The second argument to `polygon_sub` is not contained in the first one.")
		/* r.polygon([0,0], [1,0], [1,1], [0,1]).polygon_sub(r.polygon([0.1,-1], [0.9,-1], [0.9,0.5], [0.1,0.5])) */

		suite.T().Log("About to run line #90: r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0.1, -1}, []interface{}{0.9, -1}, []interface{}{0.9, 0.5}, []interface{}{0.1, 0.5}))")

		runAndAssert(suite.Suite, expected_, r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0.1, -1}, []interface{}{0.9, -1}, []interface{}{0.9, 0.5}, []interface{}{0.1, 0.5})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #90")
	}

	{
		// geo/operations.yaml line #92
		/* ({'$reql_type$':'GEOMETRY', 'coordinates':[[[0,0],[1,0],[1,1],[0,1],[0,0]],[[0,0],[0.1,0.9],[0.9,0.9],[0.9,0.1],[0,0]]], 'type':'Polygon'}) */
		var expected_ map[interface{}]interface{} = map[interface{}]interface{}{"$reql_type$": "GEOMETRY", "coordinates": []interface{}{[]interface{}{[]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}, []interface{}{0, 0}}, []interface{}{[]interface{}{0, 0}, []interface{}{0.1, 0.9}, []interface{}{0.9, 0.9}, []interface{}{0.9, 0.1}, []interface{}{0, 0}}}, "type": "Polygon"}
		/* r.polygon([0,0], [1,0], [1,1], [0,1]).polygon_sub(r.polygon([0,0],[0.1,0.9],[0.9,0.9],[0.9,0.1])) */

		suite.T().Log("About to run line #92: r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0, 0}, []interface{}{0.1, 0.9}, []interface{}{0.9, 0.9}, []interface{}{0.9, 0.1}))")

		runAndAssert(suite.Suite, expected_, r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0, 0}, []interface{}{0.1, 0.9}, []interface{}{0.9, 0.9}, []interface{}{0.9, 0.1})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #92")
	}

	{
		// geo/operations.yaml line #94
		/* err('ReqlQueryLogicError', 'Expected a Polygon with only an outer shell.  This one has holes.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "Expected a Polygon with only an outer shell.  This one has holes.")
		/* r.polygon([0,0], [1,0], [1,1], [0,1]).polygon_sub(r.polygon([0,0],[0.1,0.9],[0.9,0.9],[0.9,0.1]).polygon_sub(r.polygon([0.2,0.2],[0.5,0.8],[0.8,0.2]))) */

		suite.T().Log("About to run line #94: r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0, 0}, []interface{}{0.1, 0.9}, []interface{}{0.9, 0.9}, []interface{}{0.9, 0.1}).PolygonSub(r.Polygon([]interface{}{0.2, 0.2}, []interface{}{0.5, 0.8}, []interface{}{0.8, 0.2})))")

		runAndAssert(suite.Suite, expected_, r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Polygon([]interface{}{0, 0}, []interface{}{0.1, 0.9}, []interface{}{0.9, 0.9}, []interface{}{0.9, 0.1}).PolygonSub(r.Polygon([]interface{}{0.2, 0.2}, []interface{}{0.5, 0.8}, []interface{}{0.8, 0.2}))), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #94")
	}

	{
		// geo/operations.yaml line #96
		/* err('ReqlQueryLogicError', 'Expected a Polygon but found a LineString.', []) */
		var expected_ Err = err("ReqlQueryLogicError", "Expected a Polygon but found a LineString.")
		/* r.polygon([0,0], [1,0], [1,1], [0,1]).polygon_sub(r.line([0,0],[0.9,0.1],[0.9,0.9],[0.1,0.9])) */

		suite.T().Log("About to run line #96: r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Line([]interface{}{0, 0}, []interface{}{0.9, 0.1}, []interface{}{0.9, 0.9}, []interface{}{0.1, 0.9}))")

		runAndAssert(suite.Suite, expected_, r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1}).PolygonSub(r.Line([]interface{}{0, 0}, []interface{}{0.9, 0.1}, []interface{}{0.9, 0.9}, []interface{}{0.1, 0.9})), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #96")
	}
}
