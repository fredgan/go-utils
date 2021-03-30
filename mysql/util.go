package mysql

import (
	"unicode/utf8"
)

func Escape(sql string) string {
	dest := make([]byte, 0, 2*len(sql))
	var escape byte

	for i := 0; i < len(sql); {
		r, w := utf8.DecodeRuneInString(sql[i:])

		escape = 0

		switch r {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
			break
		case '\n': /* Must be escaped for logs */
			escape = 'n'
			break
		case '\r':
			escape = 'r'
			break
		case '\\':
			escape = '\\'
			break
		case '\'':
			escape = '\''
			break
		case '"': /* Better safe than sorry */
			escape = '"'
			break
		case '\032': /* This gives problems on Win32 */
			escape = 'Z'
		}

		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, sql[i:i+w]...)
		}

		i += w
	}

	return string(dest)
}
