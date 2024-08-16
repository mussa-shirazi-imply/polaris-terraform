package polaris

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"base_url": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("POLARIS_BASE_URL", nil),
			},
			"api_key": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("POLARIS_API_KEY", nil),
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"polaris_table":      resourcePolarisTable(),
			"polaris_connection": resourcePolarisConnection(),
		},
		ConfigureFunc: func(d *schema.ResourceData) (interface{}, error) {
			return NewClient(
				d.Get("base_url").(string),
				d.Get("api_key").(string),
			), nil
		},
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	baseURL := d.Get("base_url").(string)
	apiKey := d.Get("api_key").(string)

	client := NewClient(baseURL, apiKey)
	return client, nil
}
