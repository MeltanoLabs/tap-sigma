# tap-sigma

A Singer tap for the Sigma Computing API, built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Features

This tap extracts data from all available GET endpoints in the Sigma Computing API, including:

- **Account Management**: Account types and permissions
- **Connections**: Data connections, connection tests, and grants
- **Datasets & Data Models**: Dataset information, sources, and materializations
- **Users & Teams**: Organization members and team management
- **Files & Content**: Workbooks, pages, and files
- **Favorites & Tags**: User favorites and version tags
- **User Attributes**: Custom user attributes

## Installation

```bash
pip install tap-sigma
```

Or using Meltano:

```bash
meltano add extractor tap-sigma
```

## Configuration

The tap requires the following configuration:

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| client_id | Yes | None | Sigma Computing API Client ID |
| client_secret | Yes | None | Sigma Computing API Client Secret |
| api_url | Yes | None | Base API URL (e.g., https://aws-api.sigmacomputing.com) |
| start_date | No | None | Starting date for incremental syncs (ISO 8601) |
| stream_options | No | None | Options which change the behaviour of a specific stream (see [Stream Options](#stream-options)). |

### Stream Options

Stream options are a dictionary of stream names and their options. For example:

```json
{
  "stream_options": {
    "workbook_queries": {
      "page_size": 25
    }
  }
}
```

The available options for each stream are:

- `page_size`: The number of records to fetch per page.

### Example Configuration

Create a `config.json` file:

```json
{
  "client_id": "your-client-id",
  "client_secret": "your-client-secret",
  "api_url": "https://aws-api.sigmacomputing.com"
}
```

### Cloud Provider URLs

Sigma Computing uses different base URLs depending on your cloud provider:

- **AWS**: `https://aws-api.sigmacomputing.com`
- **Azure**: `https://azure-api.sigmacomputing.com`
- **GCP**: `https://gcp-api.sigmacomputing.com`

## Usage

### With Singer

```bash
tap-sigma --config config.json --discover > catalog.json
tap-sigma --config config.json --catalog catalog.json > output.json
```

### With Meltano

Add to your `meltano.yml`:

```yaml
extractors:
  - name: tap-sigma
    namespace: tap_sigma
    pip_url: tap-sigma
    config:
      client_id: ${SIGMA_CLIENT_ID}
      client_secret: ${SIGMA_CLIENT_SECRET}
      api_url: https://aws-api.sigmacomputing.com
```

Then run:

```bash
meltano run tap-sigma target-snowflake
```

## Available Streams

- `account_types` - List of account types
- `connections` - Data connections
- `datasets` - Dataset information
- `data_models` - Data models
- `members` - Organization members
- `teams` - Teams
- `files` - Files
- `workbooks` - Workbooks
- `workbook_pages` - Workbook pages (child stream)
- `favorites` - User favorites
- `tags` - Version tags
- `user_attributes` - User attributes
- `whoami` - Current user information

## Authentication

The tap uses OAuth 2.0 client credentials flow. It automatically handles token refresh (tokens expire after 1 hour).

## Rate Limits

The Sigma Computing API has the following rate limits:
- Standard endpoints: Reasonable request volumes
- Authentication endpoint: 1 request/second
- Export endpoints: 100 requests/minute

The tap implements automatic retry logic with exponential backoff to handle rate limiting.

## Development

### Prerequisites

- Python 3.10+
- [uv]

### Setup

```bash
git clone https://github.com/MeltanoLabs/tap-sigma
cd tap-sigma
uv sync
```

### Testing

```bash
uv run pytest
```

### Create a Test Config

```bash
cp config.sample.json config.json
# Edit config.json with your credentials
```

### Run the Tap

```bash
uv run tap-sigma --config config.json --discover
uv run tap-sigma --config config.json --catalog catalog.json
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Apache-2.0

## Resources

- [Sigma Computing API Documentation](https://help.sigmacomputing.com/reference/get-started-sigma-api)
- [Singer Specification](https://hub.meltano.com/singer/spec)
- [Meltano SDK Documentation](https://sdk.meltano.com)

[uv][https://docs.astral.sh/uv/]
