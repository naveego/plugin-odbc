package internal

import (
	"sync"
	"time"

	"context"

	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/naveego/plugin-odbc/internal/pub"
	"github.com/pkg/errors"
	"sort"
	"strings"
)

// Server type to describe a server
type Server struct {
	mu         *sync.Mutex
	log        hclog.Logger
	settings   *Settings
	db         *sql.DB
	publishing bool
	connected  bool
}

// NewServer creates a new publisher Server.
func NewServer(logger hclog.Logger) pub.PublisherServer {
	return &Server{
		mu:  &sync.Mutex{},
		log: logger,
	}
}

// Connect connects to the data base and validates the connections
func (s *Server) Connect(ctx context.Context, req *pub.ConnectRequest) (*pub.ConnectResponse, error) {
	s.log.Debug("Connecting...")
	s.settings = nil
	s.connected = false

	settings := new(Settings)
	if err := json.Unmarshal([]byte(req.SettingsJson), settings); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := settings.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	connectionString, err := settings.GetConnectionString()
	if err != nil {
		return nil, err
	}

	s.db, err = sql.Open("odbc", connectionString)
	if err != nil {
		return nil, errors.Errorf("could not open connection: %s", err)
	}

	err = s.db.Ping()
	if err != nil {
		return nil, errors.Errorf("could not ping: %s", err)
	}

	// connection made
	s.connected = true
	s.settings = settings

	s.log.Debug("Connect completed successfully.")

	return new(pub.ConnectResponse), err
}

// DiscoverShapes discovers shapes present in the database
func (s *Server) DiscoverShapes(ctx context.Context, req *pub.DiscoverShapesRequest) (*pub.DiscoverShapesResponse, error) {

	s.log.Debug("Handling DiscoverShapesRequest...")

	if !s.connected {
		return nil, errNotConnected
	}

	var shapes []*pub.Shape
	resp := &pub.DiscoverShapesResponse{}

	if req.Mode == pub.DiscoverShapesRequest_ALL {
		// return empty response
		s.log.Debug("Plugin does not support auto shape discovery")
		return resp, nil
	} 

	s.log.Debug("Refreshing schemas from request.", "count", len(req.ToRefresh))
	for _, s := range req.ToRefresh {
		shapes = append(shapes, s)
	}

	wait := new(sync.WaitGroup)

	for i := range shapes {
		shape := shapes[i]
		// include this shape in wait group
		wait.Add(1)

		// concurrently get details for shape
		go func() {
			s.log.Debug("Getting details for discovered schema...", "id", shape.Id)
			err := s.populateShapeColumns(shape)
			if err != nil {
				s.log.With("shape", shape.Id).With("err", err).Error("Error discovering columns.")
				shape.Errors = append(shape.Errors, fmt.Sprintf("Could not discover columns: %s", err))
				goto Done
			}
			s.log.Debug("Got details for discovered schema.", "id", shape.Id)

			s.log.Debug("Getting count for discovered schema...", "id", shape.Id)
			shape.Count, err = s.getCount(shape)
			if err != nil {
				s.log.With("shape", shape.Id).With("err", err).Error("Error getting row count.")
				shape.Errors = append(shape.Errors, fmt.Sprintf("Could not get row count for shape: %s", err))
				goto Done
			}
			s.log.Debug("Got count for discovered schema.", "id", shape.Id, "count", shape.Count.String())

			if req.SampleSize > 0 {
				s.log.Debug("Getting sample for discovered schema...", "id", shape.Id, "size", req.SampleSize)
				publishReq := &pub.PublishRequest{
					Shape: shape,
					Limit: req.SampleSize,
				}
				records := make(chan *pub.Record)

				go func() {
					err = s.readRecords(ctx, publishReq, records)
				}()

				for record := range records {
					shape.Sample = append(shape.Sample, record)
				}

				if err != nil {
					s.log.With("shape", shape.Id).With("err", err).Error("Error collecting sample.")
					shape.Errors = append(shape.Errors, fmt.Sprintf("Could not collect sample: %s", err))
					goto Done
				}
				s.log.Debug("Got sample for discovered schema.", "id", shape.Id, "size", len(shape.Sample))
			}
		Done:
			wait.Done()
		}()
	}

	// wait until all concurrent shape details have been loaded
	wait.Wait()

	for _, shape := range shapes {
		resp.Shapes = append(resp.Shapes, shape)
	}

	sort.Sort(pub.SortableShapes(resp.Shapes))

	return resp, nil
}

// populates the properties of the shape
func (s *Server) populateShapeColumns(shape *pub.Shape) (error) {

	query := shape.Query
	if query == "" {
		return errors.Errorf("query must be defined for shape: %s", shape)
	}

	query = strings.Replace(query, "'", "''", -1)

	rows, err := s.db.Query(query)

	if err != nil {
		return errors.Errorf("error executing query %q: %v", query, err)
	}

	if err != nil {
		return errors.WithStack(err)
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		return errors.WithStack(err)
	}

	unnamedColumnIndex := 0

	// create all named properties based on discovered column names
	for _, c := range columns {
		var property *pub.Property
		var propertyID string

		propertyName := c.Name()
		if propertyName == "" {
			propertyName = fmt.Sprintf("UNKNOWN_%d", unnamedColumnIndex)
			unnamedColumnIndex++
		}

		propertyID = fmt.Sprintf("[%s]", propertyName)

		for _, p := range shape.Properties {
			if p.Id == propertyID {
				property = p
				break
			}
		}
		if property == nil {
			property = &pub.Property{
				Id:   propertyID,
				Name: propertyName,
			}
			shape.Properties = append(shape.Properties, property)
		}

		property.TypeAtSource = c.DatabaseTypeName()

		var ok bool
		property.IsNullable, ok = c.Nullable()
		if !ok {
			property.IsNullable = true
		}
		property.IsKey = false
	}

	// determine the type of each property by reading the first data row
	properties := shape.Properties
	valueBuffer := make([]interface{}, len(properties))

	if ok := rows.Next(); ok {
		for i := range properties {
			valueBuffer[i] = &valueBuffer[i]
		}

		err = rows.Scan(valueBuffer...)
		if err != nil {
			return errors.WithStack(err)
		}

		for i, p := range properties {
			value := valueBuffer[i]

			switch value.(type) {
			case int16, int32, int64:
				p.Type = pub.PropertyType_INTEGER
			case float32, float64:
				p.Type = pub.PropertyType_FLOAT
			case bool:
				p.Type = pub.PropertyType_BOOL
			case string:
				p.Type = pub.PropertyType_STRING
			case []byte:
				if bytes, ok := value.([]byte); ok {
					str := string(bytes)
					if len(str) > 1024 {
						p.Type = pub.PropertyType_TEXT
					} else {
						p.Type = pub.PropertyType_STRING
					}
				}
			default:
				p.Type = pub.PropertyType_STRING        
			}
		}
	}

	return nil
}

// PublishStream sends records read in request to the agent
func (s *Server) PublishStream(req *pub.PublishRequest, stream pub.Publisher_PublishStreamServer) error {

	jsonReq, _ := json.Marshal(req)

	s.log.Debug("Got PublishStream request.", "req", string(jsonReq))

	if !s.connected {
		return errNotConnected
	}

	if s.settings.PrePublishQuery != "" {
		_, err := s.db.Exec(s.settings.PrePublishQuery)
		if err != nil {
			return errors.Errorf("error running pre-publish query: %s", err)
		}
	}

	var err error
	records := make(chan *pub.Record)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		readErr := s.readRecords(ctx, req, records)
		if readErr != nil {
			if err != nil {
				err = errors.Errorf("%s (publish had already stopped with error: %s)", readErr, err)
			}
		}
	}()

	for record := range records {
		sendErr := stream.Send(record)
		if sendErr != nil {
			cancel()
			err = sendErr
			break
		}
	}

	if s.settings.PostPublishQuery != "" {
		_, postPublishErr := s.db.Exec(s.settings.PostPublishQuery)
		if postPublishErr != nil {
			if err != nil {
				postPublishErr = errors.Errorf("%s (publish had already stopped with error: %s)", postPublishErr, err)
			}

			cancel()
			return errors.Errorf("error running post-publish query: %s", postPublishErr)
		}
	}

	cancel()
	return err
}

// Disconnect disconnects from the server
func (s *Server) Disconnect(context.Context, *pub.DisconnectRequest) (*pub.DisconnectResponse, error) {
	if s.db != nil {
		s.db.Close()
	}

	s.connected = false
	s.settings = nil
	s.db = nil

	return new(pub.DisconnectResponse), nil
}

// get the total number of records for a given shape
func (s *Server) getCount(shape *pub.Shape) (*pub.Count, error) {

	cErr := make(chan error)
	cCount := make(chan int)

	go func() {
		defer close(cErr)
		defer close(cCount)

		query := shape.Query
		var err error

		if query == "" {
			cErr <- errors.Errorf("query must be defined for shape: %s", shape)
			return
		} 

		rows, err := s.db.Query(query)
		if err != nil {
			cErr <- fmt.Errorf("error executing query %q: %v", query, err)
			return
		}

		count := 0
		
		for rows.Next() {
			count++
		}

		cCount <- count
	}()

	select {
	case err := <-cErr:
		return nil, err
	case count := <-cCount:
		return &pub.Count{
			Kind:  pub.Count_EXACT,
			Value: int32(count),
		}, nil
	case <-time.After(time.Second):
		return &pub.Count{
			Kind: pub.Count_UNAVAILABLE,
		}, nil
	}
}

// read and send all records for a given shape
func (s *Server) readRecords(ctx context.Context, req *pub.PublishRequest, out chan<- *pub.Record) error {

	defer close(out)

	var err error
	var query string

	query = req.Shape.Query
	if query == "" {
		return errors.Errorf("query cannot be empty")
	}

	if req.Limit > 0 {
		query = fmt.Sprintf("select top(%d) * from (%s) as q", req.Limit, query)
	}

	rows, err := s.db.Query(query)
	if err != nil {
		return errors.Errorf("error executing query %q: %v", query, err)
	}

	properties := req.Shape.Properties
	valueBuffer := make([]interface{}, len(properties))
	mapBuffer := make(map[string]interface{}, len(properties))

	for rows.Next() {
		if ctx.Err() != nil || !s.connected {
			return nil
		}

		for i, p := range properties {
			switch p.Type {
			case pub.PropertyType_FLOAT:
				var x float64
				valueBuffer[i] = &x
			case pub.PropertyType_INTEGER:
				var x int64
				valueBuffer[i] = &x
			case pub.PropertyType_DECIMAL:
				var x string
				valueBuffer[i] = &x
			default:
				valueBuffer[i] = &valueBuffer[i]
			}
		}
		err = rows.Scan(valueBuffer...)
		if err != nil {
			return errors.WithStack(err)
		}

		for i, p := range properties {
			value := valueBuffer[i]
			if str, ok := value.([]byte); ok {
				mapBuffer[p.Id] = string(str)
			} else {
				mapBuffer[p.Id] = value
			}
		}

		var record *pub.Record
		record, err = pub.NewRecord(pub.Record_UPSERT, mapBuffer)
		if err != nil {
			return errors.WithStack(err)
		}
		out <- record
	}

	return err
}

var errNotConnected = errors.New("not connected")
