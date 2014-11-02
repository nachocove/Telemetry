import re


def valid_number_str(s):
    if re.search('^0\.([0-9]+)$', s):
        return True
    if re.search('^([1-9])([0-9]*)(\.([0-9]+))?$', s):
        return True
    if s == '0':
        return True
    return False


def commafy(s):
    if not valid_number_str(s):
        raise ValueError('%s is not a valid number string' % s)
    if '.' not in s:
        idx = len(s)
    else:
        idx = s.find('.')
    if idx <= 3:
        return s  # nothing to commafy

    idx -= 3
    out = s[idx:]
    while idx >= 3:
        out = s[(idx-3):idx] + ',' + out
        idx -= 3
    if idx != 0:
        out = s[:idx] + ',' + out
    return out


def pretty_number(value, width=6, decimal_place=2):
    if not isinstance(value, float) and not isinstance(value, int):
        raise TypeError('value must be a float or an int.')

    s = str(value)
    if len(s) > width and '.' in s:
        idx = s.find('.')
        integral = s[:idx]
        fractional = s[idx+1:]

        if len(integral) >= width:
            s = integral + '.' + fractional[:decimal_place]
        else:
            s = integral + '.' + fractional[:(width - len(integral))]
    return commafy(s)