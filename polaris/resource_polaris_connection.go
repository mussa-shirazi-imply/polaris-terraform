package polaris

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"io/ioutil"
	"net/http"
)

func resourcePolarisConnection() *schema.Resource {
	return &schema.Resource{
		Create: resourcePolarisConnectionCreate,
		Read:   resourcePolarisConnectionRead,
		Update: resourcePolarisConnectionUpdate,
		Delete: resourcePolarisConnectionDelete,

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
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringInSlice([]string{"confluent", "kafka", "kinesis", "s3"}, false),
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"bootstrap_servers": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"client_rack": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"ssl": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"truststore": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"type": {
										Type:     schema.TypeString,
										Required: true,
									},
								},
							},
						},
					},
				},
			},
			"topic_name": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"topic_name_is_pattern": {
				Type:     schema.TypeBool,
				Optional: true,
			},
			"secrets": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"type": {
							Type:     schema.TypeString,
							Required: true,
						},
						"username": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"password": {
							Type:      schema.TypeString,
							Optional:  true,
							Sensitive: true,
						},
					},
				},
			},
			"aws_assumed_role_arn": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"aws_endpoint": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"stream": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"bucket": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"prefix": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}

func resourcePolarisConnectionCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(*Client)
	projectID := d.Get("project_id").(string)

	connectionType := d.Get("type").(string)
	connection := map[string]interface{}{
		"name":        d.Get("name").(string),
		"type":        connectionType,
		"description": d.Get("description").(string),
	}

	switch connectionType {
	case "confluent":
		connection["bootstrapServers"] = d.Get("bootstrap_servers").(string)
		connection["topicName"] = d.Get("topic_name").(string)
		connection["topicNameIsPattern"] = d.Get("topic_name_is_pattern").(bool)
		if v, ok := d.GetOk("secrets"); ok {
			connection["secrets"] = expandSecrets(v.([]interface{}))
		}
	case "kafka":
		connection["bootstrapServers"] = d.Get("bootstrap_servers").(string)
		connection["clientRack"] = d.Get("client_rack").(string)
		if v, ok := d.GetOk("ssl"); ok {
			connection["ssl"] = expandSSL(v.([]interface{}))
		}
		connection["topicName"] = d.Get("topic_name").(string)
		connection["topicNameIsPattern"] = d.Get("topic_name_is_pattern").(bool)
		if v, ok := d.GetOk("secrets"); ok {
			connection["secrets"] = expandSecrets(v.([]interface{}))
		}
	case "kinesis":
		connection["awsAssumedRoleArn"] = d.Get("aws_assumed_role_arn").(string)
		connection["awsEndpoint"] = d.Get("aws_endpoint").(string)
		connection["stream"] = d.Get("stream").(string)
	case "s3":
		connection["awsAssumedRoleArn"] = d.Get("aws_assumed_role_arn").(string)
		connection["awsEndpoint"] = d.Get("aws_endpoint").(string)
		connection["bucket"] = d.Get("bucket").(string)
		connection["prefix"] = d.Get("prefix").(string)
		if v, ok := d.GetOk("secrets"); ok {
			connection["secrets"] = expandSecrets(v.([]interface{}))
		}
	}

	err := client.CreateConnection(projectID, connection)
	if err != nil {
		return fmt.Errorf("Error creating connection: %s", err)
	}

	d.SetId(connection["name"].(string))
	return resourcePolarisConnectionRead(d, m)
}

func resourcePolarisConnectionRead(d *schema.ResourceData, m interface{}) error {
	client := m.(*Client)
	projectID := d.Get("project_id").(string)
	name := d.Id()

	resp, err := client.Get(fmt.Sprintf("/v1/projects/%s/connections/%s", projectID, name))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, error: %s", resp.StatusCode, string(bodyBytes))
	}

	var connection map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&connection); err != nil {
		return fmt.Errorf("Error decoding response: %s", err)
	}

	if err := d.Set("name", connection["name"]); err != nil {
		return err
	}
	if err := d.Set("type", connection["type"]); err != nil {
		return err
	}
	if err := d.Set("description", connection["description"]); err != nil {
		return err
	}

	switch connection["type"] {
	case "confluent":
		if err := d.Set("bootstrap_servers", connection["bootstrapServers"]); err != nil {
			return err
		}
		if err := d.Set("topic_name", connection["topicName"]); err != nil {
			return err
		}
		if err := d.Set("topic_name_is_pattern", connection["topicNameIsPattern"]); err != nil {
			return err
		}
		if secrets, ok := connection["secrets"].(map[string]interface{}); ok {
			if err := d.Set("secrets", []map[string]interface{}{secrets}); err != nil {
				return err
			}
		}
	case "kafka":
		if err := d.Set("bootstrap_servers", connection["bootstrapServers"]); err != nil {
			return err
		}
		if err := d.Set("client_rack", connection["clientRack"]); err != nil {
			return err
		}
		if ssl, ok := connection["ssl"].(map[string]interface{}); ok {
			if err := d.Set("ssl", []map[string]interface{}{ssl}); err != nil {
				return err
			}
		}
		if err := d.Set("topic_name", connection["topicName"]); err != nil {
			return err
		}
		if err := d.Set("topic_name_is_pattern", connection["topicNameIsPattern"]); err != nil {
			return err
		}
		if secrets, ok := connection["secrets"].(map[string]interface{}); ok {
			if err := d.Set("secrets", []map[string]interface{}{secrets}); err != nil {
				return err
			}
		}
	case "kinesis":
		if err := d.Set("aws_assumed_role_arn", connection["awsAssumedRoleArn"]); err != nil {
			return err
		}
		if err := d.Set("aws_endpoint", connection["awsEndpoint"]); err != nil {
			return err
		}
		if err := d.Set("stream", connection["stream"]); err != nil {
			return err
		}
	case "s3":
		if err := d.Set("aws_assumed_role_arn", connection["awsAssumedRoleArn"]); err != nil {
			return err
		}
		if err := d.Set("aws_endpoint", connection["awsEndpoint"]); err != nil {
			return err
		}
		if err := d.Set("bucket", connection["bucket"]); err != nil {
			return err
		}
		if err := d.Set("prefix", connection["prefix"]); err != nil {
			return err
		}
		if secrets, ok := connection["secrets"].(map[string]interface{}); ok {
			if err := d.Set("secrets", []map[string]interface{}{secrets}); err != nil {
				return err
			}
		}
	}

	return nil
}

func resourcePolarisConnectionUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(*Client)
	projectID := d.Get("project_id").(string)
	connectionName := d.Get("name").(string)

	connectionType := d.Get("type").(string)
	connection := map[string]interface{}{
		"name":        connectionName,
		"type":        connectionType,
		"description": d.Get("description").(string),
	}

	switch connectionType {
	case "confluent":
		connection["bootstrapServers"] = d.Get("bootstrap_servers").(string)
		connection["topicName"] = d.Get("topic_name").(string)
		connection["topicNameIsPattern"] = d.Get("topic_name_is_pattern").(bool)
		if v, ok := d.GetOk("secrets"); ok {
			connection["secrets"] = expandSecrets(v.([]interface{}))
		}
	case "kafka":
		connection["bootstrapServers"] = d.Get("bootstrap_servers").(string)
		connection["clientRack"] = d.Get("client_rack").(string)
		if v, ok := d.GetOk("ssl"); ok {
			connection["ssl"] = expandSSL(v.([]interface{}))
		}
		connection["topicName"] = d.Get("topic_name").(string)
		connection["topicNameIsPattern"] = d.Get("topic_name_is_pattern").(bool)
		if v, ok := d.GetOk("secrets"); ok {
			connection["secrets"] = expandSecrets(v.([]interface{}))
		}
	case "kinesis":
		connection["awsAssumedRoleArn"] = d.Get("aws_assumed_role_arn").(string)
		connection["awsEndpoint"] = d.Get("aws_endpoint").(string)
		connection["stream"] = d.Get("stream").(string)
	case "s3":
		connection["awsAssumedRoleArn"] = d.Get("aws_assumed_role_arn").(string)
		connection["awsEndpoint"] = d.Get("aws_endpoint").(string)
		connection["bucket"] = d.Get("bucket").(string)
		connection["prefix"] = d.Get("prefix").(string)
		if v, ok := d.GetOk("secrets"); ok {
			connection["secrets"] = expandSecrets(v.([]interface{}))
		}
	}

	url := fmt.Sprintf("/v1/projects/%s/connections/%s", projectID, connectionName)
	err := client.UpdateConnection(url, connection)
	if err != nil {
		return fmt.Errorf("Error updating connection: %s", err)
	}

	return resourcePolarisConnectionRead(d, m)
}

func resourcePolarisConnectionDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(*Client)
	projectID := d.Get("project_id").(string)
	connectionName := d.Get("name").(string)

	url := fmt.Sprintf("/v1/projects/%s/connections/%s", projectID, connectionName)
	err := client.DeleteConnection(url)
	if err != nil {
		return fmt.Errorf("Error deleting connection: %s", err)
	}

	d.SetId("")
	return nil
}

// resource_polaris_connection.go

func expandSecrets(secrets []interface{}) map[string]interface{} {
	if len(secrets) == 0 {
		return nil
	}
	secretMap := secrets[0].(map[string]interface{})
	expandedSecrets := map[string]interface{}{
		"type": secretMap["type"].(string),
	}
	if username, ok := secretMap["username"]; ok {
		expandedSecrets["username"] = username.(string)
	}
	if password, ok := secretMap["password"]; ok {
		expandedSecrets["password"] = password.(string)
	}
	return expandedSecrets
}

func expandSSL(ssl []interface{}) map[string]interface{} {
	if len(ssl) == 0 {
		return nil
	}
	sslMap := ssl[0].(map[string]interface{})
	expandedSSL := map[string]interface{}{
		"truststore": map[string]interface{}{
			"type": sslMap["truststore"].([]interface{})[0].(map[string]interface{})["type"].(string),
		},
	}
	return expandedSSL
}

func expandTruststore(truststore []interface{}) map[string]interface{} {
	if len(truststore) == 0 || truststore[0] == nil {
		return nil
	}

	raw := truststore[0].(map[string]interface{})
	return map[string]interface{}{
		"type": raw["type"].(string),
	}
}

func flattenSecrets(secrets map[string]interface{}) []interface{} {
	if secrets == nil {
		return nil
	}

	return []interface{}{
		map[string]interface{}{
			"type": secrets["type"].(string),
		},
	}
}

func flattenSSL(ssl map[string]interface{}) []interface{} {
	if ssl == nil {
		return nil
	}

	return []interface{}{
		map[string]interface{}{
			"truststore": flattenTruststore(ssl["truststore"].(map[string]interface{})),
		},
	}
}

func flattenTruststore(truststore map[string]interface{}) []interface{} {
	if truststore == nil {
		return nil
	}

	return []interface{}{
		map[string]interface{}{
			"type": truststore["type"].(string),
		},
	}
}
