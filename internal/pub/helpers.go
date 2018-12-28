package pub

import (
	"context"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"github.com/hashicorp/go-hclog"
	"log"
	"encoding/json"
	"encoding"
	"fmt"
	"strings"
)

type ShapeMetadata struct {

}

type PropertyMetadata struct {

}

type SortableShapes []*Shape

func (s SortableShapes) Len() int {
	return len(s)
}

func (s SortableShapes) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortableShapes) Less(i, j int) bool {
	s1Name := s[i].Id
	s2Name := s[j].Id
	return strings.Compare(s1Name, s2Name) < 0
}

type SortableProperties []*Property

func (s SortableProperties) Len() int {
	return len(s)
}

func (s SortableProperties) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortableProperties) Less(i, j int) bool {
	s1Name := s[i].Id
	s2Name := s[j].Id
	return strings.Compare(s1Name, s2Name) < 0
}



func (c *Count) Format() string {
	if c == nil {
		return "unavailable"
	}

	switch c.Kind {
	case Count_UNAVAILABLE:
		return "unavailable"
	case Count_ESTIMATE:
		return fmt.Sprintf("~%d", c.Value)
	case Count_EXACT:
		return fmt.Sprintf("%d", c.Value)
	default:
		return fmt.Sprintf("unknown kind: %s", c.Kind)
	}
}

func NewConnectRequest(settings interface{}) (*ConnectRequest) {
	b, err := json.Marshal(settings)
	if err != nil {
		return &ConnectRequest{}
	}

	req := &ConnectRequest{
		SettingsJson: string(b),
	}

	return req
}

func NewRecord(action Record_Action, data interface{}) (*Record, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	r := &Record{
		Action:   action,
		DataJson: string(b),
	}

	return r, nil
}

// NewServerPlugin returns a plugin.Plugin for use as a server.
// This is the method to call from the plugin implementation
// when creating a new plugin.ServeConfig.
func NewServerPlugin(handler PublisherServer) plugin.Plugin {
	return &publisherPlugin{
		handler: handler,
	}
}

// NewClientPlugin returns a plugin.Plugin for use as a client.
// This is the method to call from the host when creating a new plugin.ClientConfig.
func NewClientPlugin(log *logrus.Entry) plugin.Plugin {
	return &publisherPlugin{
		log: log,
	}
}

type publisherPlugin struct {
	plugin.NetRPCUnsupportedPlugin
	log     *logrus.Entry
	handler PublisherServer
}

func (p *publisherPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	RegisterPublisherServer(s, p.handler)
	return nil
}

func (p *publisherPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return NewPublisherClient(c), nil
}

var _ plugin.GRPCPlugin = &publisherPlugin{}
var _ plugin.Plugin = &publisherPlugin{}



func AdaptHCLog(log *logrus.Entry) hclog.Logger {
	return &hclLogAdapter{
		log: log,
	}
}

type hclLogAdapter struct {
	log *logrus.Entry
}

func (l *hclLogAdapter) Trace(msg string, args ...interface{}) {
	l.toLogrus(hclog.Trace, msg, args...)
}

func (l *hclLogAdapter) Debug(msg string, args ...interface{}) {
	l.toLogrus(hclog.Debug, msg, args...)
}

func (l *hclLogAdapter) Info(msg string, args ...interface{}) {
	l.toLogrus(hclog.Info, msg, args...)
}

func (l *hclLogAdapter) Warn(msg string, args ...interface{}) {
	l.toLogrus(hclog.Warn, msg, args...)
}

func (l *hclLogAdapter) Error(msg string, args ...interface{}) {
	l.toLogrus(hclog.Error, msg, args...)
}

func (l *hclLogAdapter) toLogrus(level hclog.Level, msg string, args ...interface{}) {

	fields := makeFields(args...)
	e := l.log.WithFields(fields)

	switch level {
	case hclog.Error:
		e.Error(msg)
	case hclog.Warn:
		e.Warn(msg)
	case hclog.Info:
		e.Info(msg)
	default:
		e.Debug(msg)
	}
}

func getLogrusLevel(level hclog.Level) logrus.Level {
	switch level {
	case hclog.Error:
		return logrus.ErrorLevel
	case hclog.Warn:
		return logrus.WarnLevel
	case hclog.Info:
		return logrus.InfoLevel
	default:
		return logrus.DebugLevel
	}
}

func makeFields(args ...interface{}) logrus.Fields {
	fields := logrus.Fields{}

	for i := 0; i < len(args); i = i + 2 {
		if _, ok := args[i].(string); !ok {
			// As this is the logging function not much we can do here
			// without injecting into logs...
			continue
		}
		val := args[i+1]
		switch sv := val.(type) {
		case error:
			// Check if val is of type error. If error type doesn't
			// implement json.Marshaler or encoding.TextMarshaler
			// then set val to err.Error() so that it gets marshaled
			switch sv.(type) {
			case json.Marshaler, encoding.TextMarshaler:
			default:
				val = sv.Error()
			}
		}

		fields[args[i].(string)] = val
	}

	return fields
}

func (l *hclLogAdapter) IsTrace() bool {
	return l.log.Level >= logrus.DebugLevel
}

func (l *hclLogAdapter) IsDebug() bool {
	return l.log.Level >= logrus.DebugLevel
}

func (l *hclLogAdapter) IsInfo() bool {
	return l.log.Level >= logrus.InfoLevel
}

func (l *hclLogAdapter) IsWarn() bool {
	return l.log.Level >= logrus.WarnLevel
}

func (l *hclLogAdapter) IsError() bool {
	return l.log.Level >= logrus.ErrorLevel
}

func (l *hclLogAdapter) With(args ...interface{}) hclog.Logger {
	return &hclLogAdapter{
		log: l.log.WithFields(makeFields(args)),
	}
}

func (l *hclLogAdapter) Named(name string) hclog.Logger {
	return &hclLogAdapter{
		log: l.log.WithField("name", name),
	}
}

func (l *hclLogAdapter) ResetNamed(name string) hclog.Logger {
	return &hclLogAdapter{
		log: l.log.WithField("name", name),
	}
}

func (l *hclLogAdapter) SetLevel(level hclog.Level) {
	l.log.Logger.SetLevel(getLogrusLevel(level))
}

func (l *hclLogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	panic("not implemented")
}

