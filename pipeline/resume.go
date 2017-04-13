package pipeline

import (
	"encoding/json"
	"io"

	"github.com/compose/mejson"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/message"
)

type resumeData struct {
	offset uint64
	ns     string
	msg    client.MessageSet
}

func readResumeData(r io.Reader) (resumeData, error) {
	rd := resumeData{}
	logOffset, entry, err := commitlog.ReadEntry(r)
	if err != nil {
		return rd, err
	}
	rd.offset = logOffset
	rd.ns = string(entry.Key)
	d := make(map[string]interface{})
	if err := json.Unmarshal(entry.Value, &d); err != nil {
		return resumeData{}, err
	}
	data, err := mejson.Unmarshal(d)
	if err != nil {
		return resumeData{}, err
	}
	rd.msg = client.MessageSet{
		Msg:       message.From(entry.Op, string(entry.Key), map[string]interface{}(data)),
		Timestamp: int64(entry.Timestamp),
		Mode:      entry.Mode,
	}
	return rd, nil
}
