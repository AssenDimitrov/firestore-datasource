package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"strings"
	

	"cloud.google.com/go/firestore"
	vkit "cloud.google.com/go/firestore/apiv1"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/pgollangi/fireql"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

func NewDatasource(_ backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	return &Datasource{}, nil
}

type Datasource struct{}

func (d *Datasource) Dispose() {
	// Clean up datasource instance resources.
}

func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	log.DefaultLogger.Debug("QueryData called", "numQueries", len(req.Queries))

	response := backend.NewQueryDataResponse()

	for _, q := range req.Queries {
		res := d.query(ctx, req.PluginContext, q)
		response.Responses[q.RefID] = res
	}

	return response, nil
}

type FirestoreQuery struct {
	Query string
}

type FirestoreSettings struct {
	ProjectId    string
	DatabaseName string
}

func (d *Datasource) query(ctx context.Context, pCtx backend.PluginContext, query backend.DataQuery) (response backend.DataResponse) {
	defer func() {
		if err := recover(); err != nil {
			log.DefaultLogger.Error("panic occurred ", err)
			response = backend.ErrDataResponse(backend.StatusInternal, "internal server error")
		}
	}()
	response = d.queryInternal(ctx, pCtx, query)
	return response
}


////////////////////////////////////

func (d *Datasource) queryInternal(ctx context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	var response backend.DataResponse

	// Unmarshal the JSON into our queryModel.
	var qm FirestoreQuery
	err := json.Unmarshal(query.JSON, &qm)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, "json unmarshal: "+err.Error())
	}
	log.DefaultLogger.Debug("FirestoreQuery: ", qm)

	var settings FirestoreSettings
	err = json.Unmarshal(pCtx.DataSourceInstanceSettings.JSONData, &settings)
	if err != nil {
		log.DefaultLogger.Error("Error parsing settings ", err)
		return backend.ErrDataResponse(backend.StatusBadRequest, "ProjectID: "+err.Error())
	}

	if len(settings.ProjectId) == 0 {
		return backend.ErrDataResponse(backend.StatusBadRequest, "ProjectID is required")
	}

	var options []fireql.Option
	if pCtx.DataSourceInstanceSettings.DecryptedSecureJSONData["serviceAccount"] != "" {
		options = append(options, fireql.OptionServiceAccount(pCtx.DataSourceInstanceSettings.DecryptedSecureJSONData["serviceAccount"]))
	}

	if settings.DatabaseName != "" {
		options = append(options, fireql.OptionDatabaseName(settings.DatabaseName))
	}

	fQuery, err := fireql.New(settings.ProjectId, options...)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, "fireql.NewFireQL: "+err.Error())
	}

	log.DefaultLogger.Info("Created fireql.NewFireQLWithServiceAccountJSON")

	if len(qm.Query) > 0 {
		log.DefaultLogger.Info("Executing query", qm.Query)
		result, err := fQuery.Execute(qm.Query)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadRequest, "fireql.Execute: "+err.Error())
		}

		// Create data frame response
		frame := data.NewFrame("response")

		// Add a new column for document ID
		docIDField := data.NewField("__document_id", nil, make([]*string, len(result.Records)))
		frame.Fields = append(frame.Fields, docIDField)

		// Determine the maximum number of fields across all records
		maxFields := 0
		for _, record := range result.Records {
			if len(record) > maxFields {
				maxFields = len(record)
			}
		}

		// Create fields with nil values for missing fields
		for i := 0; i < maxFields; i++ {
			var fieldName string
			if i < len(result.Columns) {
				fieldName = result.Columns[i]
			} else {
				fieldName = fmt.Sprintf("field_%d", i+1)
			}

			field := data.NewField(fieldName, nil, make([]*string, len(result.Records)))
			frame.Fields = append(frame.Fields, field)
		}

		// Populate field values for each record
		for rowIdx, record := range result.Records {
			// Extract document ID
			var docID string
			for colIdx, value := range record {
				if colIdx < len(result.Columns) && strings.ToLower(result.Columns[colIdx]) == "__name__" {
					if strValue, ok := value.(string); ok {
						parts := strings.Split(strValue, "/")
						docID = parts[len(parts)-1]
					}
					break
				}
			}
			frame.Fields[0].Set(rowIdx, &docID)

			for colIdx := 0; colIdx < maxFields; colIdx++ {
				fieldIdx := colIdx + 1 // +1 because we added the document ID field
				if colIdx < len(record) {
					value := record[colIdx]
					if timeValue, ok := value.(time.Time); ok {
						// Convert time.Time to a string representation
						strValue := timeValue.Format(time.RFC3339)
						frame.Fields[fieldIdx].Set(rowIdx, &strValue)
					} else if strValue, ok := value.(string); ok {
						frame.Fields[fieldIdx].Set(rowIdx, &strValue)
					} else {
						// Convert other types to string representation
						strValue := fmt.Sprintf("%v", value)
						frame.Fields[fieldIdx].Set(rowIdx, &strValue)
					}
				} else {
					frame.Fields[fieldIdx].Set(rowIdx, nil)
				}
			}
		}

		// Add the frame to the response
		response.Frames = append(response.Frames, frame)
	}

	return response
}

//////////////////////////////////

func createTypedField(name string, values []interface{}, length int) (*data.Field, error) {
	if len(values) == 0 {
		return data.NewField(name, nil, make([]string, length)), nil
	}

	var (
		boolVals   = make([]*bool, length)
		intVals    = make([]*int64, length)
		floatVals  = make([]*float64, length)
		stringVals = make([]*string, length)
		timeVals   = make([]*time.Time, length)
	)

	allBool := true
	allInt := true
	allFloat := true
	allTime := true

	for i := 0; i < length; i++ {
		if i >= len(values) {
			// Handle case when i is out of range for values
			break
		}

		v := values[i]
		switch val := v.(type) {
		case bool:
			boolVals[i] = &val
		case int, int32, int64:
			intVal := val.(int64) // Type assertion to int64
			intVals[i] = &intVal
		case float32, float64:
			floatVal := val.(float64) // Type assertion to float64
			floatVals[i] = &floatVal
			allInt = false
		case string:
			stringVals[i] = &val
			allBool = false
			allInt = false
			allFloat = false
			allTime = false
		case time.Time:
			timeVals[i] = &val
			allBool = false
			allInt = false
			allFloat = false
		case nil:
			// Handle null values
		default:
			// For complex types, convert to JSON string
			jsonVal, err := json.Marshal(val)
			if err != nil {
				return nil, fmt.Errorf("error marshaling value to JSON: %v", err)
			}
			strVal := string(jsonVal)
			stringVals[i] = &strVal
			allBool = false
			allInt = false
			allFloat = false
			allTime = false
		}
	}

	if allBool {
		return data.NewField(name, nil, boolVals), nil
	}
	if allInt {
		return data.NewField(name, nil, intVals), nil
	}
	if allFloat {
		return data.NewField(name, nil, floatVals), nil
	}
	if allTime {
		return data.NewField(name, nil, timeVals), nil
	}

	return data.NewField(name, nil, stringVals), nil
}



///////////////////////////////////////////

func newFirestoreClient(ctx context.Context, pCtx backend.PluginContext) (*firestore.Client, error) {
	var settings FirestoreSettings
	err := json.Unmarshal(pCtx.DataSourceInstanceSettings.JSONData, &settings)
	if err != nil {
		log.DefaultLogger.Error("Error parsing settings ", err)
		return nil, fmt.Errorf("ProjectID: %v", err)
	}

	if len(settings.ProjectId) == 0 {
		return nil, errors.New("project Id is required")
	}

	var options []option.ClientOption
	serviceAccount := pCtx.DataSourceInstanceSettings.DecryptedSecureJSONData["serviceAccount"]

	if len(serviceAccount) > 0 {
		if !json.Valid([]byte(serviceAccount)) {
			return nil, errors.New("invalid service account, it is expected to be a JSON")
		}
		creds, err := google.CredentialsFromJSON(ctx, []byte(serviceAccount),
			vkit.DefaultAuthScopes()...,
		)
		if err != nil {
			log.DefaultLogger.Error("google.CredentialsFromJSON ", err)
			return nil, fmt.Errorf("ServiceAccount: %v", err)
		}
		options = append(options, option.WithCredentials(creds))
	}

	client, err := firestore.NewClientWithDatabase(ctx, settings.ProjectId, settings.DatabaseName, options...)

	if err != nil {
		log.DefaultLogger.Error("firestore.NewClient ", err)
		return nil, fmt.Errorf("firestore.NewClient: %v", err)
	}
	return client, nil
}

func (d *Datasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	log.DefaultLogger.Debug("CheckHealth called")

	var status = backend.HealthStatusOk
	var message = "Data source is working"

	client, healthErr := newFirestoreClient(ctx, req.PluginContext)

	if healthErr == nil {
		defer client.Close()
		collections := client.Collections(ctx)
		collection, err := collections.Next()
		if err == nil || errors.Is(err, iterator.Done) {
			log.DefaultLogger.Debug("First collections: ", collection.ID)
		} else {
			log.DefaultLogger.Error("client.Collections ", err)
			healthErr = fmt.Errorf("firestore.Collections: %v", err)
		}
	}

	if healthErr != nil {
		status = backend.HealthStatusError
		message = healthErr.Error()
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}