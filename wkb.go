package pgpush

// specification at http://edndoc.esri.com/arcsde/9.1/general_topics/wkb_representation.htm

import (
	"bytes"
	"encoding/binary"
	"github.com/paulmach/go.geojson"
	"io"
	"math"
)

const (
	pointType              uint32 = 1
	lineStringType         uint32 = 2
	polygonType            uint32 = 3
	multiPointType         uint32 = 4
	multiLineStringType    uint32 = 5
	multiPolygonType       uint32 = 6
	geometryCollectionType uint32 = 7
)

// DefaultByteOrder is the order used form marshalling or encoding
// is none is specified.
var DefaultByteOrder binary.ByteOrder = binary.LittleEndian

// An Encoder will encode a geometry as WKB to the writer given at
// creation time.
type Encoder struct {
	buf []byte

	w     io.Writer
	order binary.ByteOrder
}

func (e *Encoder) writePoint(pp *geojson.Geometry) error {
	p := pp.Point
	e.order.PutUint32(e.buf, pointType)
	_, err := e.w.Write(e.buf[:4])
	if err != nil {
		return err
	}

	e.order.PutUint64(e.buf, math.Float64bits(p[0]))
	e.order.PutUint64(e.buf[8:], math.Float64bits(p[1]))
	_, err = e.w.Write(e.buf)
	return err
}

func (e *Encoder) writeMultiPoint(mpp *geojson.Geometry) error {
	mp := mpp.MultiPoint
	e.order.PutUint32(e.buf, multiPointType)
	e.order.PutUint32(e.buf[4:], uint32(len(mp)))
	_, err := e.w.Write(e.buf[:8])
	if err != nil {
		return err
	}

	for _, p := range mp {
		err := e.Encode(geojson.NewPointGeometry(p))
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) writeLineString(lss *geojson.Geometry) error {
	ls := lss.LineString
	e.order.PutUint32(e.buf, lineStringType)
	e.order.PutUint32(e.buf[4:], uint32(len(ls)))
	_, err := e.w.Write(e.buf[:8])
	if err != nil {
		return err
	}

	for _, p := range ls {
		e.order.PutUint64(e.buf, math.Float64bits(p[0]))
		e.order.PutUint64(e.buf[8:], math.Float64bits(p[1]))
		_, err = e.w.Write(e.buf)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) writeMultiLineString(mlss *geojson.Geometry) error {
	mls := mlss.MultiLineString
	e.order.PutUint32(e.buf, multiLineStringType)
	e.order.PutUint32(e.buf[4:], uint32(len(mls)))
	_, err := e.w.Write(e.buf[:8])
	if err != nil {
		return err
	}

	for _, ls := range mls {
		err := e.Encode(geojson.NewLineStringGeometry(ls))
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) writePolygon(pp *geojson.Geometry) error {
	for i := range pp.Polygon {
		f, l := pp.Polygon[i][0], pp.Polygon[i][len(pp.Polygon[i])-1]
		if !(f[0] == l[0] && f[1] == l[1]) {
			pp.Polygon[i] = append(pp.Polygon[i], pp.Polygon[i][0])
		}
	}
	p := pp.Polygon

	e.order.PutUint32(e.buf, polygonType)
	e.order.PutUint32(e.buf[4:], uint32(len(p)))
	_, err := e.w.Write(e.buf[:8])
	if err != nil {
		return err
	}
	for _, r := range p {
		e.order.PutUint32(e.buf, uint32(len(r)))
		_, err := e.w.Write(e.buf[:4])
		if err != nil {
			return err
		}
		for _, p := range r {
			e.order.PutUint64(e.buf, math.Float64bits(p[0]))
			e.order.PutUint64(e.buf[8:], math.Float64bits(p[1]))
			_, err = e.w.Write(e.buf)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Encoder) writeMultiPolygon(mpp *geojson.Geometry) error {
	mp := mpp.MultiPolygon
	e.order.PutUint32(e.buf, multiPolygonType)
	e.order.PutUint32(e.buf[4:], uint32(len(mp)))
	_, err := e.w.Write(e.buf[:8])
	if err != nil {
		return err
	}

	for _, p := range mp {
		err := e.Encode(geojson.NewPolygonGeometry(p))
		if err != nil {
			return err
		}
	}

	return nil
}

// Encode will write the geometry encoded as WKB to the given writer.
func (e *Encoder) Encode(geom *geojson.Geometry) error {
	if geom == nil {
		return nil
	}

	var b []byte
	if e.order == binary.LittleEndian {
		b = []byte{1}
	} else {
		b = []byte{0}
	}

	_, err := e.w.Write(b)
	if err != nil {
		return err
	}

	if e.buf == nil {
		e.buf = make([]byte, 16)
	}

	switch geom.Type {
	case "Point":
		return e.writePoint(geom)
	case "MultiPoint":
		return e.writeMultiPoint(geom)
	case "LineString":
		return e.writeLineString(geom)
	case "MultiLineString":
		return e.writeMultiLineString(geom)
	case "Polygon":
		return e.writePolygon(geom)
	case "MultiPolygon":
		return e.writeMultiPolygon(geom)

	}

	panic("unsupported type")
}

// NewEncoder creates a new Encoder for the given writer
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w:     w,
		order: DefaultByteOrder,
	}
}

//
func EncodeGeometryWKB(geom *geojson.Geometry) ([]byte, error) {
	var b bytes.Buffer
	w := io.Writer(&b)
	e := NewEncoder(w)
	err := e.Encode(geom)
	return b.Bytes(), err
}
