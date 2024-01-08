package timestamp

import "time"

func Now() uint64 {
	return ToUnit64(time.Now())
}

func ToUnit64(ts time.Time) uint64 {
	return uint64(ts.UnixNano())
}

func IsValidTs(ts time.Time, ttl time.Duration) bool {
	lastValidTs := time.Now().Add(-1 * ttl)
	return ts.After(lastValidTs)
}

func IsValidTsUint(ts uint64, ttl time.Duration) bool {
	lastValidTs := time.Now().Add(-1 * ttl)
	lastValidTsUint := ToUnit64(lastValidTs)

	return ts > lastValidTsUint
}
