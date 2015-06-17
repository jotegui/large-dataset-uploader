# GBIF upload, full process

[toc]

## GETting the latest GBIF data

* Execute a call to [http://api.gbif.org/v1/dataset?q=ebird](http://api.gbif.org/v1/dataset?q=ebird) to look for eBird's `datasetKey`
* Fill in the template with a valid `username`, list of email addresses for notification, and eBird's `datasetKey` and save it as `filter.json`

```json
{
  "creator":"username",
  "notification_address": ["notification_addresses"],
  "predicate":
  {
    "type":"not",
        "predicate":
        {
          "type":"equals",
          "key":"DATASET_KEY",
          "value":"eBird_datasetKey"
        }
  }
}

```

* Launch an API download request with the `filter.json` filter. It returns a `downloadKey` which can be checked using the downloads API

      curl -i -u username -H "Content-type: application/json" -X POST --data @filter.json http://api.gbif.org/v1/occurrence/download/request

	  Return example: 0003270-150528172251762