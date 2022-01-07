// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"encoding/binary"
	"encoding/hex"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
	"github.com/twpayne/go-geom/xy"
	"math"
)

var (
	_ functionClass = &pointFunctionClass{}
)

var (
	_ builtinFunc = &builtinPointStringSig{}
)

type builtinPointStringSig struct {
	baseBuiltinFunc
}

func (b *builtinPointStringSig) Clone() builtinFunc {
	newSig := &builtinPointStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinPointStringSig) evalString(row chunk.Row) (string, bool, error) {
	return b.evalStringWithCtx(b.ctx, row)
}

func (b *builtinPointStringSig) evalStringWithCtx(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {
	val1, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETInt, val1), err) {
		// Return 0 for invalid date time.
		return "", false, nil
	}
	if isNull {
		return "", true, nil
	}

	val2, isNull, err := b.args[1].EvalDecimal(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETInt, val2), err) {
		// Return 0 for invalid date time.
		return "", false, nil
	}
	if isNull {
		return "", true, nil
	}

	fVal1, err := val1.ToFloat64()
	fVal2, err := val2.ToFloat64()
	// POINT() function logic
	point := geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{fVal1, fVal2})
	//pointStr, err := wkt.Marshal(point)
	pointBytes, err := wkb.Marshal(point, binary.LittleEndian)
	byteStr := "0x" + hex.EncodeToString(pointBytes)

	if err != nil {
		return "", false, err
	}

	return byteStr, false, nil
}

type pointFunctionClass struct {
	baseFunctionClass
}

func (c *pointFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETDecimal, types.ETDecimal)
	if err != nil {
		return nil, err
	}
	sig := &builtinPointStringSig{bf}
	sig.setPbCode(6200)
	return sig, nil
}

type builtinSTEqualsIntSig struct {
	baseBuiltinFunc
}

func (b *builtinSTEqualsIntSig) Clone() builtinFunc {
	newSig := &builtinSTEqualsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSTEqualsIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	return b.evalIntWithCtx(b.ctx, row)
}

func (b *builtinSTEqualsIntSig) evalIntWithCtx(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	val1, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETString, val1), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	val2, isNull, err := b.args[1].EvalString(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETString, val2), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	// ST_Equals function logic
	intVal := int64(0)
	if val1 == val2 {
		intVal = 1
	}
	return intVal, false, nil
}

type stEqualsFunctionClass struct {
	baseFunctionClass
}

func (c *stEqualsFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinSTEqualsIntSig{bf}
	sig.setPbCode(6201)
	return sig, nil
}

type builtinSTDistanceDecSig struct {
	baseBuiltinFunc
}

func (b *builtinSTDistanceDecSig) Clone() builtinFunc {
	newSig := &builtinSTDistanceDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSTDistanceDecSig) evalReal(row chunk.Row) (float64, bool, error) {
	return b.evalRealWithCtx(b.ctx, row)
}

func (b *builtinSTDistanceDecSig) evalRealWithCtx(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	geoByteStr1, isNull, err := b.args[0].EvalString(ctx, row)
	val1, isNull, err := getGeoDatum(geoByteStr1)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETString, val1), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	geoByteStr2, isNull, err := b.args[1].EvalString(ctx, row)
	val2, isNull, err := getGeoDatum(geoByteStr2)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETString, val2), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	// ST_Distance function logic

	lineString1 := geom.NewLineStringFlat(val1.Layout(), val1.FlatCoords())
	lineString2 := geom.NewLineStringFlat(val2.Layout(), val2.FlatCoords())
	distance := xy.Distance(val1.FlatCoords(), val2.FlatCoords())
	len1 := len(lineString1.Coords())
	len2 := len(lineString2.Coords())
	for i := 0; i < len1; i++ {
		line1Start := lineString1.Coord(i)
		line1End := line1Start
		if i+1 < len1 {
			line1End = lineString1.Coord(i + 1)
		}
		for j := 0; j < len2; j++ {
			line2Start := lineString2.Coord(j)
			line2End := line2Start
			if j+1 < len2 {
				line2End = lineString2.Coord(j + 1)
			}

			distance1 := xy.DistanceFromLineToLine(line1Start, line1End, line2Start, line2End)
			distance = math.Min(distance, distance1)
		}

	}
	return distance, false, nil
}

type stDistanceFunctionClass struct {
	baseFunctionClass
}

func (c *stDistanceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinSTDistanceDecSig{bf}
	sig.setPbCode(6202)
	return sig, nil
}

type builtinSTXDecSig struct {
	baseBuiltinFunc
}

func (b *builtinSTXDecSig) Clone() builtinFunc {
	newSig := &builtinSTXDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSTXDecSig) evalReal(row chunk.Row) (float64, bool, error) {
	return b.evalRealWithCtx(b.ctx, row)
}

func (b *builtinSTXDecSig) evalRealWithCtx(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	geoByteStr1, isNull, err := b.args[0].EvalString(ctx, row)
	val1, isNull, err := getGeoDatum(geoByteStr1)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETString, val1), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}
	// ST_Distance function logic

	point := geom.NewPoint(val1.Layout()).MustSetCoords(val1.FlatCoords())

	return point.X(), false, nil
}

type stXFunctionClass struct {
	baseFunctionClass
}

func (c *stXFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinSTXDecSig{bf}
	sig.setPbCode(6203)
	return sig, nil
}

type builtinSTYDecSig struct {
	baseBuiltinFunc
}

func (b *builtinSTYDecSig) Clone() builtinFunc {
	newSig := &builtinSTYDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSTYDecSig) evalReal(row chunk.Row) (float64, bool, error) {
	return b.evalRealWithCtx(b.ctx, row)
}

func (b *builtinSTYDecSig) evalRealWithCtx(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	geoByteStr1, isNull, err := b.args[0].EvalString(ctx, row)
	val1, isNull, err := getGeoDatum(geoByteStr1)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETString, val1), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}
	// ST_Distance function logic

	point := geom.NewPoint(val1.Layout()).MustSetCoords(val1.FlatCoords())

	return point.Y(), false, nil
}

type stYFunctionClass struct {
	baseFunctionClass
}

func (c *stYFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinSTYDecSig{bf}
	sig.setPbCode(6204)
	return sig, nil
}

type builtinLineStringStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLineStringStringSig) Clone() builtinFunc {
	newSig := &builtinLineStringStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLineStringStringSig) evalString(row chunk.Row) (string, bool, error) {
	return b.evalStringWithCtx(b.ctx, row)
}

func (b *builtinLineStringStringSig) evalStringWithCtx(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {

	coordArr := make([]geom.Coord, 0, len(b.args))
	for i := 0; i < len(b.args); i++ {
		geoByteStr, isNull, err := b.args[i].EvalString(ctx, row)
		val, isNull, err := getGeoDatum(geoByteStr)
		if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETString, val), err) {
			// Return 0 for invalid date time.
			return "", false, nil
		}
		if isNull {
			return "", true, nil
		}
		point := geom.NewPoint(val.Layout()).MustSetCoords(val.FlatCoords())
		coordArr = append(coordArr, point.FlatCoords())
	}

	// POINT() function logic
	lineString := geom.NewLineString(geom.XY).MustSetCoords(coordArr)

	lineStringBytes, err := wkb.Marshal(lineString, binary.LittleEndian)
	byteStr := "0x" + hex.EncodeToString(lineStringBytes)
	if err != nil {
		return "", false, err
	}

	return byteStr, false, nil
}

type LineStringFunctionClass struct {
	baseFunctionClass
}

func (c *LineStringFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinLineStringStringSig{bf}
	sig.setPbCode(6300)
	return sig, nil
}

type builtinSTLengthDecSig struct {
	baseBuiltinFunc
}

func (b *builtinSTLengthDecSig) Clone() builtinFunc {
	newSig := &builtinSTLengthDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSTLengthDecSig) evalReal(row chunk.Row) (float64, bool, error) {
	return b.evalRealWithCtx(b.ctx, row)
}

func (b *builtinSTLengthDecSig) evalRealWithCtx(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	geoByteStr1, isNull, err := b.args[0].EvalString(ctx, row)
	val1, isNull, err := getGeoDatum(geoByteStr1)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETString, val1), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	lineString1 := geom.NewLineStringFlat(val1.Layout(), val1.FlatCoords())

	return lineString1.Length(), false, nil
}

type stLengthFunctionClass struct {
	baseFunctionClass
}

func (c *stLengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinSTLengthDecSig{bf}
	sig.setPbCode(6301)
	return sig, nil
}

type builtinSTAsTextSigIntSig struct {
	baseBuiltinFunc
}

func (b *builtinSTAsTextSigIntSig) Clone() builtinFunc {
	newSig := &builtinSTAsTextSigIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSTAsTextSigIntSig) evalString(row chunk.Row) (string, bool, error) {
	return b.evalStringWithCtx(b.ctx, row)
}

func (b *builtinSTAsTextSigIntSig) evalStringWithCtx(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {
	geoByteStr, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETString, geoByteStr), err) {
		// Return "" for invalid column name
		return "", false, nil
	}
	if isNull {
		return "", true, nil
	}
	geoObj, _, err := getGeoDatum(geoByteStr)
	if err != nil {
		return "", false, err
	}
	geoStr, err := wkt.Marshal(geoObj)
	if err != nil {
		return "", false, err
	}
	return geoStr, false, nil
}

type stAsTextFunctionClass struct {
	baseFunctionClass
}

func (c *stAsTextFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinSTAsTextSigIntSig{bf}
	sig.setPbCode(6202)
	return sig, nil
}

func getGeoDatum(geoByteStr string) (geom.T, bool, error) {

	// ST_AsText function logic
	// "0x....." -> "POINT(1 1)"
	geoBytes, err := hex.DecodeString(geoByteStr[2:])
	if err != nil {
		return nil, false, err
	}

	geoObj, err := wkb.Unmarshal(geoBytes)
	//geoStr, err := wkt.Marshal(geoObj)
	return geoObj, false, err
}

type builtinSTGeomFromTextSigStringSig struct {
	baseBuiltinFunc
}

func (b *builtinSTGeomFromTextSigStringSig) Clone() builtinFunc {
	newSig := &builtinSTGeomFromTextSigStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSTGeomFromTextSigStringSig) evalString(row chunk.Row) (string, bool, error) {
	return b.evalStringWithCtx(b.ctx, row)
}

func (b *builtinSTGeomFromTextSigStringSig) evalStringWithCtx(ctx sessionctx.Context, row chunk.Row) (string, bool, error) {
	wktStr, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETString, wktStr), err) {
		// Return "" for invalid column name
		return "", false, nil
	}
	if isNull {
		return "", true, nil
	}

	geoObj, err := wkt.Unmarshal(wktStr)
	if err != nil {
		return "", false, err
	}

	geoBytes, err := wkb.Marshal(geoObj, binary.LittleEndian)
	if err != nil {
		return "", false, err
	}

	byteStr := "0x" + hex.EncodeToString(geoBytes)
	return byteStr, false, nil
}

type stGeomFromTextFunctionClass struct {
	baseFunctionClass
}

func (c *stGeomFromTextFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinSTGeomFromTextSigStringSig{bf}
	sig.setPbCode(6202)
	return sig, nil
}
