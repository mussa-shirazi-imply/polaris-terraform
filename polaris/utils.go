package polaris

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Table struct {
	ID                      string            `json:"id"`
	Name                    string            `json:"name"`
	Type                    string            `json:"type"`
	Version                 int               `json:"version"`
	Description             *string           `json:"description,omitempty"`
	ClusteringColumns       *[]string         `json:"clusteringColumns,omitempty"`
	PartitioningGranularity string            `json:"partitioningGranularity"`
	QueryGranularity        *QueryGranularity `json:"queryGranularity,omitempty"`
	Schema                  []SchemaColumn    `json:"schema"`
	SchemaMode              string            `json:"schemaMode"`
	StoragePolicy           *StoragePolicy    `json:"storagePolicy,omitempty"`
	TimeResolution          string            `json:"timeResolution"`
	Availability            string            `json:"availability"`
	CreatedByUser           *User             `json:"createdByUser,omitempty"`
	CreatedOnTimestamp      string            `json:"createdOnTimestamp"`
	ModifiedByUser          *User             `json:"modifiedByUser,omitempty"`
	ModifiedOnTimestamp     string            `json:"modifiedOnTimestamp"`
	SegmentCompactedBytes   int               `json:"segmentCompactedBytes"`
	SegmentTotalBytes       int               `json:"segmentTotalBytes"`
	TotalDataSizeBytes      int               `json:"totalDataSizeBytes"`
	TotalRows               int               `json:"totalRows"`
	QueryableSchema         []SchemaColumn    `json:"queryableSchema"`
}

type StoragePolicy struct {
	Cached *StoragePolicyDetail `json:"cached,omitempty"`
	Retain *StoragePolicyDetail `json:"retain,omitempty"`
}

type StoragePolicyDetail struct {
	Type      string   `json:"type"`
	Intervals []string `json:"intervals,omitempty"`
}

type QueryGranularity struct {
	Type string `json:"type"`
}

type SchemaColumn struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	DataType   string `json:"dataType"`
	PrimaryKey bool   `json:"primaryKey,omitempty"`
}

type User struct {
	Username string `json:"username"`
	UserID   string `json:"userId"`
}

type ErrorResponse struct {
	Code       string                `json:"code"`
	Message    string                `json:"message"`
	Details    []ErrorResponseDetail `json:"details"`
	InnerError InnerError            `json:"innererror"`
	Target     string                `json:"target"`
}

type ErrorResponseDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Target  string `json:"target"`
}

type InnerError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func parseErrorResponse(response *http.Response) string {
	var errorResponse ErrorResponse
	if err := json.NewDecoder(response.Body).Decode(&errorResponse); err != nil {
		return "Unknown error"
	}

	errorMessage := fmt.Sprintf("Error: %s - %s", errorResponse.Code, errorResponse.Message)

	if len(errorResponse.Details) > 0 {
		errorMessage += "\nDetails:"
		for _, detail := range errorResponse.Details {
			errorMessage += fmt.Sprintf("\n - %s: %s", detail.Code, detail.Message)
		}
	}

	if errorResponse.InnerError.Code != "" {
		errorMessage += fmt.Sprintf("\nInner Error: %s - %s", errorResponse.InnerError.Code, errorResponse.InnerError.Message)
	}

	return errorMessage
}
