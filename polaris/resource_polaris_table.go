package polaris

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"net/http"
)

func resourcePolarisTable() *schema.Resource {
	return &schema.Resource{
		Create: resourcePolarisTableCreate,
		Read:   resourcePolarisTableRead,
		Update: resourcePolarisTableUpdate,
		Delete: resourcePolarisTableDelete,

		Schema: map[string]*schema.Schema{
			"project_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"type": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"version": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
			},
			"clustering_columns": {
				Type:     schema.TypeList,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"partitioning_granularity": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "day",
			},
			"query_granularity": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"schema": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"type": {
							Type:     schema.TypeString,
							Required: true,
						},
						"data_type": {
							Type:     schema.TypeString,
							Required: true,
						},
						"primary_key": {
							Type:     schema.TypeBool,
							Optional: true,
						},
					},
				},
			},
			"schema_mode": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"storage_policy": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"cached": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"type": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"intervals": {
										Type:     schema.TypeString,
										Optional: true,
									},
								},
							},
						},
						"retain": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"type": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"intervals": {
										Type:     schema.TypeString,
										Optional: true,
									},
								},
							},
						},
					},
				},
			},

			"time_resolution": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "millisecond",
			},
			"availability": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "available",
			},
			"created_by_user": {
				Type:     schema.TypeMap,
				Computed: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"created_on_timestamp": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"id": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"modified_by_user": {
				Type:     schema.TypeMap,
				Computed: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"modified_on_timestamp": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"segment_compacted_bytes": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"segment_total_bytes": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"total_data_size_bytes": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"total_rows": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"queryable_schema": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"type": {
							Type:     schema.TypeString,
							Required: true,
						},
						"data_type": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
		},
	}
}

func resourcePolarisTableCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(*Client)
	projectID := d.Get("project_id").(string)
	tableName := d.Get("name").(string)

	// Check if the table already exists
	exists, err := tableExists(client, projectID, tableName)
	if err != nil {
		return fmt.Errorf("Error checking if table exists: %s", err)
	}
	if exists {
		return fmt.Errorf("Table already exists")
	}

	var queryGranularity map[string]interface{}
	if v, ok := d.GetOk("query_granularity"); ok {
		queryGranularity = v.(map[string]interface{})
	} else {
		queryGranularity = nil
	}

	var storagePolicy []interface{}
	if v, ok := d.GetOk("storage_policy"); ok {
		storagePolicy = v.([]interface{})
	} else {
		storagePolicy = nil
	}

	table := Table{
		Name:                    d.Get("name").(string),
		Type:                    d.Get("type").(string),
		Version:                 d.Get("version").(int),
		Description:             getStringPointer(d, "description"),
		ClusteringColumns:       getStringListPointer(d, "clustering_columns"),
		PartitioningGranularity: d.Get("partitioning_granularity").(string),
		QueryGranularity:        expandQueryGranularity(queryGranularity),
		Schema:                  expandSchema(d.Get("schema").([]interface{})),
		SchemaMode:              d.Get("schema_mode").(string),
		StoragePolicy:           expandStoragePolicy(storagePolicy),
		TimeResolution:          d.Get("time_resolution").(string),
		Availability:            d.Get("availability").(string),
	}
	log.Printf("[DEBUG] Created table with ID&&&&&&&&&&&&&&&&&&&&&&&: %s", table.ID)

	err = client.CreateTable(projectID, &table)
	if err != nil {
		return fmt.Errorf("Error creating table: %s", err)
	}

	log.Printf("[DEBUG] Created table with ID&&&&&&&&&&&&&&&&&&&&&&&&: %s", table.ID)
	d.SetId(table.ID)                     // Ensure the resource ID is set
	return resourcePolarisTableRead(d, m) // Read the resource state to ensure consistency
}

func convertSchemaColumnsToSchema(columns []SchemaColumn) []Schema {
	schema := make([]Schema, len(columns))
	for i, col := range columns {
		schema[i] = Schema{
			Type:     col.Type,
			DataType: col.DataType,
			Name:     col.Name,
		}
	}
	return schema
}

func convertSchemaColumnsToQueryableSchema(columns []SchemaColumn) []QueryableSchema {
	queryableSchema := make([]QueryableSchema, len(columns))
	for i, col := range columns {
		queryableSchema[i] = QueryableSchema{
			Name: col.Name,
		}
	}
	return queryableSchema
}

func resourcePolarisTableRead(d *schema.ResourceData, m interface{}) error {
	client := m.(*Client)
	projectID := d.Get("project_id").(string)
	tableName := d.Get("name").(string) // Use table name instead of table ID

	url := fmt.Sprintf("/v1/projects/%s/tables/%s", projectID, tableName)
	response, err := client.Get(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNotFound {
		log.Print("[DEBUG]Not Found ^^^^^^^^^^^^^^^^^^^: %s", http.StatusNotFound)
		d.SetId("")
		return nil
	}

	if response.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("Unauthorized: Please check your API key and permissions")
	}

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Unexpected status code: %d", response.StatusCode)
	}

	var table Table
	if err := json.NewDecoder(response.Body).Decode(&table); err != nil {
		return err
	}

	log.Printf("[DEBUG] Read table: %+v", table)

	d.Set("name", table.Name)
	d.Set("type", table.Type)
	d.Set("description", table.Description)
	d.Set("version", table.Version)
	d.Set("clustering_columns", table.ClusteringColumns)
	d.Set("partitioning_granularity", table.PartitioningGranularity)
	d.Set("query_granularity", flattenQueryGranularity(table.QueryGranularity))
	d.Set("schema", flattenSchema(convertSchemaColumnsToSchema(table.Schema)))
	d.Set("schema_mode", table.SchemaMode)
	d.Set("storage_policy", flattenStoragePolicy(table.StoragePolicy))
	d.Set("time_resolution", table.TimeResolution)
	d.Set("availability", table.Availability)
	d.Set("created_by_user", flattenUser(table.CreatedByUser))
	d.Set("created_on_timestamp", table.CreatedOnTimestamp)
	d.Set("id", table.ID)
	d.Set("modified_by_user", flattenUser(table.ModifiedByUser))
	d.Set("modified_on_timestamp", table.ModifiedOnTimestamp)
	d.Set("segment_compacted_bytes", table.SegmentCompactedBytes)
	d.Set("segment_total_bytes", table.SegmentTotalBytes)
	d.Set("total_data_size_bytes", table.TotalDataSizeBytes)
	d.Set("total_rows", table.TotalRows)
	d.Set("queryable_schema", flattenQueryableSchema(convertSchemaColumnsToQueryableSchema(table.QueryableSchema)))

	log.Printf("[DEBUG] Finished reading table with ID: %s, State: %+v", tableName, d.State())

	return nil
}

func flattenQueryGranularity(qg *QueryGranularity) map[string]interface{} {
	if qg == nil {
		return nil
	}

	return map[string]interface{}{
		"type": qg.Type,
	}
}

type Schema struct {
	Type     string `json:"type"`
	DataType string `json:"dataType"`
	Name     string `json:"name"`
}

func flattenSchema(schema []Schema) []map[string]interface{} {
	if schema == nil {
		return nil
	}

	flatSchema := make([]map[string]interface{}, len(schema))
	for i, s := range schema {
		flatSchema[i] = map[string]interface{}{
			"type":     s.Type,
			"dataType": s.DataType,
			"name":     s.Name,
		}
	}

	return flatSchema
}

func flattenStoragePolicy(sp *StoragePolicy) []map[string]interface{} {
	if sp == nil {
		return nil
	}

	return []map[string]interface{}{
		{
			"cached": map[string]interface{}{
				"type": sp.Cached.Type,
			},
			"retain": map[string]interface{}{
				"type": sp.Retain.Type,
			},
		},
	}
}

func flattenUser(user *User) map[string]interface{} {
	if user == nil {
		return nil
	}

	return map[string]interface{}{
		"username": user.Username,
		"user_id":  user.UserID,
	}
}

// Define the QueryableSchema type
type QueryableSchema struct {
	Name string `json:"name"`
}

func flattenQueryableSchema(qs []QueryableSchema) []map[string]interface{} {
	if qs == nil {
		return nil
	}

	flatQueryableSchema := make([]map[string]interface{}, len(qs))
	for i, q := range qs {
		flatQueryableSchema[i] = map[string]interface{}{
			"name": q.Name,
		}
	}

	return flatQueryableSchema
}

func resourcePolarisTableUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(*Client)
	projectID := d.Get("project_id").(string)
	tableID := d.Id()

	var storagePolicy []interface{}
	if v, ok := d.GetOk("storage_policy"); ok {
		storagePolicy = v.([]interface{})
	} else {
		storagePolicy = nil
	}

	table := Table{
		Name:                    d.Get("name").(string),
		Type:                    d.Get("type").(string),
		Description:             getStringPointer(d, "description"),
		Version:                 d.Get("version").(int),
		ClusteringColumns:       getStringListPointer(d, "clustering_columns"),
		PartitioningGranularity: d.Get("partitioning_granularity").(string),
		QueryGranularity:        expandQueryGranularity(d.Get("query_granularity").(map[string]interface{})),
		Schema:                  expandSchema(d.Get("schema").([]interface{})),
		SchemaMode:              d.Get("schema_mode").(string),
		StoragePolicy:           expandStoragePolicy(storagePolicy),
		TimeResolution:          d.Get("time_resolution").(string),
		Availability:            d.Get("availability").(string),
	}

	url := fmt.Sprintf("/v1/projects/%s/tables/%s", projectID, tableID)
	response, err := client.Put(url, table)
	if err != nil {
		return fmt.Errorf("Error updating table: %s", err)
	}
	defer response.Body.Close()

	log.Printf("[DEBUG] Updated table with ID: %s", tableID)
	return resourcePolarisTableRead(d, m) // Read the resource state to ensure consistency
}

func resourcePolarisTableDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(*Client)
	projectID := d.Get("project_id").(string)
	tableID := d.Id()

	url := fmt.Sprintf("/v1/projects/%s/tables/%s", projectID, tableID)
	req, err := http.NewRequest("DELETE", client.baseURL+url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+client.apiKey)

	response, err := client.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("Unexpected status code: %d", response.StatusCode)
	}

	d.SetId("")
	return nil
}

func tableExists(client *Client, projectID, tableName string) (bool, error) {
	url := fmt.Sprintf("/v1/projects/%s/tables", projectID)
	response, err := client.Get(url)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusUnauthorized {
		return false, fmt.Errorf("Unauthorized: Please check your API key and permissions")
	}

	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("Unexpected status code: %d", response.StatusCode)
	}

	var tablesResponse struct {
		Values []Table `json:"values"`
	}
	if err := json.NewDecoder(response.Body).Decode(&tablesResponse); err != nil {
		return false, err
	}

	for _, table := range tablesResponse.Values {
		if table.Name == tableName {
			return true, nil
		}
	}
	return false, nil
}

func getStringPointer(d *schema.ResourceData, key string) *string {
	if v, ok := d.GetOk(key); ok {
		value := v.(string)
		return &value
	}
	return nil
}

func getStringListPointer(d *schema.ResourceData, key string) *[]string {
	if v, ok := d.GetOk(key); ok {
		var result []string
		for _, item := range v.([]interface{}) {
			result = append(result, item.(string))
		}
		return &result
	}
	return nil
}

func expandQueryGranularity(data map[string]interface{}) *QueryGranularity {
	if data == nil {
		return nil
	}
	qg := &QueryGranularity{}
	if v, ok := data["type"].(string); ok {
		qg.Type = v
	}
	return qg
}

func expandSchema(data []interface{}) []SchemaColumn {
	var schema []SchemaColumn
	for _, item := range data {
		fieldData := item.(map[string]interface{})
		field := SchemaColumn{
			Name:       fieldData["name"].(string),
			Type:       fieldData["type"].(string),
			DataType:   fieldData["data_type"].(string),
			PrimaryKey: fieldData["primary_key"].(bool),
		}
		schema = append(schema, field)
	}
	return schema
}

func expandStoragePolicy(data []interface{}) *StoragePolicy {
	if len(data) == 0 || data[0] == nil {
		return nil
	}

	spMap := data[0].(map[string]interface{})
	sp := &StoragePolicy{}

	if cached, ok := spMap["cached"].([]interface{}); ok && len(cached) > 0 {
		sp.Cached = expandStoragePolicyDetail(cached[0].(map[string]interface{}))
	}
	if retain, ok := spMap["retain"].([]interface{}); ok && len(retain) > 0 {
		sp.Retain = expandStoragePolicyDetail(retain[0].(map[string]interface{}))
	}

	return sp
}

func expandStoragePolicyDetail(data map[string]interface{}) *StoragePolicyDetail {
	if data == nil {
		return nil
	}
	spd := &StoragePolicyDetail{}
	if v, ok := data["type"].(string); ok {
		spd.Type = v
	}
	if v, ok := data["intervals"].([]interface{}); ok {
		var intervals []string
		for _, interval := range v {
			intervals = append(intervals, interval.(string))
		}
		spd.Intervals = intervals
	}
	return spd
}
