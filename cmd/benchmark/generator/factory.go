package generator

type Distribution int

const (
	SEQUENTIAL Distribution = iota
	UNIFORM
)

func Build(dist Distribution, start int64, count int64) Generator {

	var keyRangeLowerBound = start
	var keyRangeUpperBound = start + count - 1

	var keygen Generator
	switch dist {
	case UNIFORM:
		keygen = NewUniform(keyRangeLowerBound, keyRangeUpperBound)
	case SEQUENTIAL:
		keygen = NewSequential(keyRangeLowerBound, keyRangeUpperBound)
	default:
		panic("Unknown distribution")
	}
	return keygen
}
