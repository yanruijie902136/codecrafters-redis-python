_MIN_LATITUDE = -85.05112878
_MAX_LATITUDE = 85.05112878
_MIN_LONGITUDE = -180
_MAX_LONGITUDE = 180

_LATITUDE_RANGE = _MAX_LATITUDE - _MIN_LATITUDE
_LONGITUDE_RANGE = _MAX_LONGITUDE - _MIN_LONGITUDE


def is_valid_location(longitude: float, latitude: float) -> bool:
    return _MIN_LONGITUDE <= longitude <= _MAX_LONGITUDE and _MIN_LATITUDE <= latitude <= _MAX_LATITUDE


def compute_score(longitude: float, latitude: float) -> int:
    normalized_longitude = 2**26 * (longitude - _MIN_LONGITUDE) / _LONGITUDE_RANGE
    normalized_longitude = int(normalized_longitude)

    normalized_latitude = 2**26 * (latitude - _MIN_LATITUDE) / _LATITUDE_RANGE
    normalized_latitude = int(normalized_latitude)

    return _interleave(normalized_latitude, normalized_longitude)


def _interleave(x: int, y: int) -> int:
    x = _spread_int32_to_int64(x)
    y = _spread_int32_to_int64(y)

    y_shifted = y << 1
    return x | y_shifted


def _spread_int32_to_int64(v: int) -> int:
    v = v & 0xFFFFFFFF

    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2)) & 0x3333333333333333
    v = (v | (v << 1)) & 0x5555555555555555

    return v
