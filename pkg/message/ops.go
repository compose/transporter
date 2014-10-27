package message

/*
 * some basic op types
 */
type OpType int

const (
	Insert OpType = iota
	Update
	Delete
	Command
	Unknown
)

func (o OpType) String() string {
	switch o {
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	case Command:
		return "command"
	default:
		return "unknown"
	}
}

func OpTypeFromString(s string) OpType {
	switch s[0] {
	case 'i':
		return Insert
	case 'u':
		return Update
	case 'd':
		return Delete
	case 'c':
		return Command
	default:
		return Unknown
	}
}

/*
 * different types of commands
 */
type CommandType int

const (
	Flush CommandType = iota
)
