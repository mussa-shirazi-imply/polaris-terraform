package polaris

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL:    baseURL,
		apiKey:     apiKey,
		httpClient: &http.Client{},
	}
}

// Post sends a POST request to the Polaris API using Basic Authorization.
func (client *Client) Post(url string, body interface{}) (*http.Response, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", client.baseURL+url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	auth := base64.StdEncoding.EncodeToString([]byte(client.apiKey + ":"))
	req.Header.Set("Authorization", "Basic "+auth)

	return client.httpClient.Do(req)
}

func (client *Client) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", client.baseURL+url, nil)
	if err != nil {
		return nil, err
	}
	auth := base64.StdEncoding.EncodeToString([]byte(client.apiKey + ":"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Basic "+auth)

	fmt.Printf("Request URL: %s\n", client.baseURL+url)
	fmt.Printf("Authorization: Basic %s\n", auth)

	return client.httpClient.Do(req)
}

// Put sends a PUT request to the Polaris API.
func (client *Client) Put(url string, body interface{}) (*http.Response, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PUT", client.baseURL+url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+client.apiKey)

	return client.httpClient.Do(req)
}

func (c *Client) CreateTable(projectID string, table *Table) error {
	url := fmt.Sprintf("%s/v1/projects/%s/tables", c.baseURL, projectID)
	jsonData, err := json.Marshal(table)
	if err != nil {
		return fmt.Errorf("Error marshaling table: %s", err)
	}

	fmt.Printf("Request URL: %s\n", url)
	fmt.Printf("Request Payload: %s\n", string(jsonData))

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("Error creating request: %s", err)
	}

	req.Header.Set("Content-Type", "application/json")
	auth := base64.StdEncoding.EncodeToString([]byte(c.apiKey + ":"))
	req.Header.Set("Authorization", "Basic "+auth)

	fmt.Println("Headers set, sending request...")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Error making request: %s", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	bodyString := string(bodyBytes)

	fmt.Printf("Response Status Code: %d\n", resp.StatusCode)
	fmt.Printf("Response Body: %s\n", bodyString)

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d, error: %s", resp.StatusCode, bodyString)
	}

	// Decode the response body into the table object to extract the ID
	if err := json.NewDecoder(bytes.NewBuffer(bodyBytes)).Decode(table); err != nil {
		return fmt.Errorf("Error decoding response=============================================: %s", err)
	}

	// Ensure the table ID is set
	if table.ID == "" {
		return fmt.Errorf("Table ID not set in response===============================")
	}
	log.Printf("[DEBUG] Created table with ID&£££££££££££££££££££££££££££££££££££££££££££££££££££: %s", table.ID)
	fmt.Printf("Decoded Table ID£££££££££££££££££££££££££££££££££££££££££££££££££££: %s\n", table.ID)
	return nil
}

func (client *Client) CreateConnection(projectID string, connection map[string]interface{}) error {
	url := fmt.Sprintf("/v1/projects/%s/connections", projectID)
	resp, err := client.Post(url, connection)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	bodyString := string(bodyBytes)

	fmt.Printf("Response Status Code: %d\n", resp.StatusCode)
	fmt.Printf("Response Body: %s\n", bodyString)

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d, error: %s", resp.StatusCode, bodyString)
	}

	return nil
}

func (client *Client) UpdateConnection(url string, connection map[string]interface{}) error {
	resp, err := client.Put(url, connection)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, error: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func (client *Client) DeleteConnection(url string) error {
	req, err := http.NewRequest("DELETE", client.baseURL+url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+client.apiKey)

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, error: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func (client *Client) CreateJob(projectID string, job map[string]interface{}) (string, error) {
	url := fmt.Sprintf("/v1/projects/%s/jobs", projectID)
	resp, err := client.Post(url, job)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	bodyString := string(bodyBytes)

	fmt.Printf("Response Status Code: %d\n", resp.StatusCode)
	fmt.Printf("Response Body: %s\n", bodyString)

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("unexpected status code: %d, error: %s", resp.StatusCode, bodyString)
	}

	// Parse the response to extract the job ID
	var result map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return "", fmt.Errorf("error parsing response body: %s", err)
	}

	jobID, ok := result["id"].(string)
	if !ok {
		return "", fmt.Errorf("job ID not found in response")
	}

	return jobID, nil
}

// extractIDFromLocation extracts the table ID from the Location header.
func extractIDFromLocation(location string) string {
	parts := strings.Split(location, "/")
	return parts[len(parts)-1]
}
