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
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"math"
)

var (
	_ functionClass = &pointFunctionClass{}
)

var (
	_ builtinFunc = &builtinPointSigIntSig{}
)

type builtinPointSigIntSig struct {
	baseBuiltinFunc
}

func (b *builtinPointSigIntSig) Clone() builtinFunc {
	newSig := &builtinPointSigIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinPointSigIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	return b.evalIntWithCtx(b.ctx, row)
}

func (b *builtinPointSigIntSig) evalIntWithCtx(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	val1, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETInt, val1), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	val2, isNull, err := b.args[1].EvalInt(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETInt, val2), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	intVal := val1*1000 + val2
	return intVal, false, nil
}

type pointFunctionClass struct {
	baseFunctionClass
}

func (c *pointFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinPointSigIntSig{bf}
	sig.setPbCode(6200)
	return sig, nil
}

type builtinSTEqualsSigIntSig struct {
	baseBuiltinFunc
}

func (b *builtinSTEqualsSigIntSig) Clone() builtinFunc {
	newSig := &builtinSTEqualsSigIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSTEqualsSigIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	return b.evalIntWithCtx(b.ctx, row)
}

func (b *builtinSTEqualsSigIntSig) evalIntWithCtx(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	val1, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETInt, val1), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	val2, isNull, err := b.args[1].EvalInt(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETInt, val2), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

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
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinSTEqualsSigIntSig{bf}
	sig.setPbCode(6201)
	return sig, nil
}

type builtinSTDistanceSigIntSig struct {
	baseBuiltinFunc
}

func (b *builtinSTDistanceSigIntSig) Clone() builtinFunc {
	newSig := &builtinSTDistanceSigIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSTDistanceSigIntSig) evalReal(row chunk.Row) (float64, bool, error) {
	return b.evalRealWithCtx(b.ctx, row)
}

func (b *builtinSTDistanceSigIntSig) evalRealWithCtx(ctx sessionctx.Context, row chunk.Row) (float64, bool, error) {
	val1, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETInt, val1), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	val2, isNull, err := b.args[1].EvalInt(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.ETInt, val2), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	realX := 1.0 * float64(val1/1000-val2/1000)
	realY := 1.0 * float64(val1%1000-val2%1000)
	realVal := math.Sqrt(realX*realX + realY*realY)
	return realVal, false, nil
}

type stDistanceFunctionClass struct {
	baseFunctionClass
}

func (c *stDistanceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinSTDistanceSigIntSig{bf}
	sig.setPbCode(6202)
	return sig, nil
}
