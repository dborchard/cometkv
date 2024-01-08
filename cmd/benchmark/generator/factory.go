package generator

type Distribution int

const (
	SEQUENTIAL Distribution = iota
	UNIFORM
)

func Build(dist Distribution, start int64, count int64) Generator {

	var keyrangeLowerBound = start
	var keyrangeUpperBound = start + count - 1

	var keygen Generator
	switch dist {
	case UNIFORM:
		keygen = NewUniform(keyrangeLowerBound, keyrangeUpperBound)
	case SEQUENTIAL:
		keygen = NewSequential(keyrangeLowerBound, keyrangeUpperBound)
	default:
		panic("Unknown distribution")
	}
	return keygen
}
